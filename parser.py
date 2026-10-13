"""Nit: interpretador incremental de JSON.

API principal:
    parser = JSONStreamParser(schema=MeuBaseModel)  # schema opcional
    parser.feed(chunk)
    parser.has('content')
    parser.listen('content')  # somente o trecho NOVO já decodificado
    parser.get('content')     # valor acumulado
    obj = parser.finish()     # valida, retorna resultado e LIMPA o stream
    parser.abort()            # limpa um stream cancelado/incompleto

Um parser derivado pode observar um ramo do mesmo stream:
    props = parser.extract('props', schema=MinhasProps)
    parser.feed(chunk)        # props é atualizado automaticamente
    props.listen('content')   # caminhos são relativos a props

`listen` em strings entrega apenas novos caracteres decodificados.
`listen` em arrays entrega uma lista com os novos elementos COMPLETOS.
Para acessar texto ainda em geração dentro do array, use `listen('actions[0].content')`.
Outros tipos retornam '' (use `get`).
Caminhos como 'actions[0].content' são aceitos.
Uma instância armazena apenas o estado temporário de UM stream ativo.
Para streams concorrentes, crie instâncias separadas.
"""

from __future__ import annotations

import json
import re
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from pydantic import BaseModel


class _State(Enum):
    VALUE = auto()
    KEY_OR_END = auto()
    COLON = auto()
    STRING = auto()
    NUMBER = auto()
    LITERAL = auto()
    COMMA_OR_END = auto()
    DONE = auto()


@dataclass
class _Frame:
    kind: str  # object | array
    value: dict | list
    key: str | None = None
    allow_end: bool = True  # False após vírgula (impede vírgula final)
    path: tuple[str | int, ...] = ()


_MISSING = object()
_NUMBER = re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z')
_PATH = re.compile(r'[^.\[\]]+|\[[0-9]+\]')
_ESCAPES = {'"': '"', '\\': '\\', '/': '/', 'b': '\b',
            'f': '\f', 'n': '\n', 'r': '\r', 't': '\t'}
_LITERALS = {'true': True, 'false': False, 'null': None}


class JSONStreamParser[T: BaseModel]:
    def __init__(
        self,
        schema: type[T] | None = None,
        *,
        _source: JSONStreamParser | None = None,
        _prefix: tuple[str | int, ...] = (),
    ):
        if schema is not None and (
            not isinstance(schema, type) or not issubclass(schema, BaseModel)
        ):
            raise TypeError('schema deve ser uma classe derivada de pydantic.BaseModel')

        self.schema = schema
        self._source = _source
        self._prefix = _prefix
        if _source is None:
            self.reset()
        else:
            # Cada visão possui cursores próprios, embora leia a mesma árvore.
            self._cursors: dict[tuple[str | int, ...], int] = {}

    @property
    def data(self) -> Any:
        """Estrutura parcial visível para este parser."""
        if self._source is None:
            return self._data
        value = self._source._lookup_path(self._prefix)
        return None if value is _MISSING else value

    @data.setter
    def data(self, value: Any) -> None:
        self._data = value

    def extract[S: BaseModel](
        self, key: str, schema: type[S] | None = None
    ) -> JSONStreamParser[S]:
        """Cria uma visão tipada e conectada a um ramo deste parser.

        A visão não recebe cópias nem precisa ser alimentada: toda chamada a
        ``feed`` no parser de origem fica imediatamente disponível nela. Seus
        caminhos públicos são relativos ao ramo extraído.
        """
        return JSONStreamParser(
            schema=schema,
            _source=self._root,
            _prefix=(*self._prefix, *self._path(key)),
        )

    @property
    def _root(self) -> JSONStreamParser:
        return self if self._source is None else self._source._root

    def reset(self) -> None:
        """Descarta integralmente o estado transitório do stream atual.

        O schema configurado no construtor é preservado. Pode ser chamado
        antes de reutilizar a instância ou depois de interromper uma geração.
        """
        if self._source is not None:
            self._cursors.clear()
            return

        # A estrutura parcial é JSON puro, não uma instância do BaseModel.
        # Models com campos obrigatórios não podem ser instanciados vazios.
        # A validação Pydantic acontece exclusivamente em finish().
        self.data: Any = None
        self._state = _State.VALUE
        self._frames: list[_Frame] = []
        self._cursors: dict[tuple[str | int, ...], int] = {}
        self._completed: set[tuple[str | int, ...]] = set()
        self._value_path: tuple[str | int, ...] = ()

        self._text = ''
        self._literal = ''
        self._string_is_key = False
        self._string_value = ''
        self._string_pending: list[str] = []
        self._string_parent: _Frame | None = None
        self._string_index: int | None = None
        self._string_key: str | None = None
        self._escaped = False
        self._unicode_digits: str | None = None
        self._high_surrogate: int | None = None

    def abort(self) -> None:
        """Cancela o stream em andamento sem validá-lo."""
        if self._source is not None:
            self._source.abort()
            self._cursors.clear()
            return
        self.reset()

    @property
    def is_done(self) -> bool:
        """Indica se o documento JSON terminou sintaticamente."""
        if self._source is not None:
            return self._prefix in self._root._completed
        return self._state == _State.DONE

    def feed(self, chunk: str) -> Any:
        """Consome fragmentos de JSON, preservando o estado entre chamadas."""
        if self._source is not None:
            self._source.feed(chunk)
            return self.data
        if not isinstance(chunk, str):
            raise TypeError('feed espera uma string')

        try:
            for char in chunk:
                self._consume(char)
            self._flush_string()
            return self.data
        except Exception:
            # Após uma falha, nenhum estado inválido pode vazar para outro stream.
            self.reset()
            raise

    def has(self, key: str) -> bool:
        """True quando o caminho já possui um valor no mapa parcial."""
        return self._lookup(key) is not _MISSING

    def get(self, key: str, default: Any = None) -> Any:
        """Valor acumulado (inclusive strings ainda incompletas)."""
        result = self._lookup(key)
        return default if result is _MISSING else result

    def listen(self, key: str) -> str | list[Any]:
        """Consome somente os dados NOVOS do campo solicitado.

        - str: devolve novos caracteres já decodificados; não duplica texto.
        - list: devolve novos itens inteiramente fechados (um único envio por item).
        - outros tipos / campo ausente: devolve ''.

        ATENÇÃO: para streaming de texto dentro de um item ainda aberto, use
        listen('actions[0].content'), sem aguardar o fechamento do objeto.
        """
        value = self._lookup(key)
        path = (*self._prefix, *self._path(key))
        if isinstance(value, str):
            cursor = self._cursors.get(path, 0)
            delta = value[cursor:]
            self._cursors[path] = len(value)
            return delta

        if isinstance(value, list):
            cursor = self._cursors.get(path, 0)
            stop = cursor
            while stop < len(value) and (*path, stop) in self._root._completed:
                stop += 1
            # Cópias isolam o consumidor de alterações no mapa parcial.
            delta = deepcopy(value[cursor:stop])
            self._cursors[path] = stop
            return delta

        return ''

    def stream(self, key: str) -> str | list[Any]:
        """Alias de compatibilidade para listen()."""
        return self.listen(key)

    def is_complete(self, key: str) -> bool:
        """True só depois que o valor inteiro deste caminho foi fechado."""
        return (*self._prefix, *self._path(key)) in self._root._completed

    def finish(self) -> T:
        """Valida o documento e retorna o resultado, limpando o estado.

        Sempre limpa o stream, mesmo se houver JSON incompleto ou um erro
        de validação do Pydantic. A instância fica pronta para o próximo JSON.
        O objeto retornado continua válido após a limpeza.
        """
        if self._source is not None:
            if not self.is_done:
                raise ValueError('JSON incompleto')
            result = self.data
            if self.schema is not None:
                result = self.schema.model_validate(result)
            return result

        try:
            if self._state == _State.NUMBER:
                self._finish_number()
            if self._state != _State.DONE:
                raise ValueError('JSON incompleto')

            result: T = self.data
            if self.schema is not None:
                result = self.schema.model_validate(result)
            return result
        finally:
            self.reset()

    def _path(self, key: str) -> tuple[str | int, ...]:
        if not isinstance(key, str) or not key:
            raise ValueError("key deve ser uma string não vazia")
        if isinstance(self.data, dict) and key in self.data:
            return (key,)
        tokens = _PATH.findall(key)
        if not tokens:
            return (key,)
        return tuple(int(t[1:-1]) if t.startswith("[") else t for t in tokens)

    def _next_path(self) -> tuple[str | int, ...]:
        if not self._frames:
            return ()
        frame = self._frames[-1]
        segment = frame.key if frame.kind == "object" else len(frame.value)
        return (*frame.path, segment)

    def _lookup(self, key: str) -> Any:
        if not isinstance(key, str) or not key:
            raise ValueError('key deve ser uma string não vazia')

        # Preserva acesso a chaves literais contendo pontos/colchetes.
        visible = self.data
        if isinstance(visible, dict) and key in visible:
            return visible[key]

        parts = _PATH.findall(key)
        if not parts:
            return _MISSING
        return self._root._lookup_path((*self._prefix, *(
            int(token[1:-1]) if token.startswith('[') else token
            for token in parts
        )))

    def _lookup_path(self, parts: tuple[str | int, ...]) -> Any:
        current = self.data
        for token in parts:
            if isinstance(token, int):
                if not isinstance(current, list):
                    return _MISSING
                if token >= len(current):
                    return _MISSING
                current = current[token]
            elif isinstance(current, dict):
                if token not in current:
                    return _MISSING
                current = current[token]
            elif isinstance(current, list) and token.isdecimal():
                i = int(token)
                if i >= len(current):
                    return _MISSING
                current = current[i]
            else:
                return _MISSING
        return current

    def _assign(self, value: Any) -> None:
        if not self._frames:
            self.data = value
            return
        frame = self._frames[-1]
        if frame.kind == 'object':
            assert frame.key is not None
            frame.value[frame.key] = value
        else:
            frame.value.append(value)

    def _start_string(self, *, is_key: bool) -> None:
        self._state = _State.STRING
        self._string_is_key = is_key
        self._string_value = ''
        self._string_pending.clear()
        self._escaped = False
        self._unicode_digits = None
        self._high_surrogate = None
        self._string_parent = self._frames[-1] if self._frames else None
        if not is_key:
            self._value_path = self._next_path()
        self._string_key = (
            self._string_parent.key
            if self._string_parent is not None and self._string_parent.kind == 'object'
            else None
        )
        self._string_index = (
            len(self._string_parent.value)
            if self._string_parent is not None and self._string_parent.kind == 'array'
            else None
        )
        if not is_key:
            self._assign('')

    def _flush_string(self) -> None:
        if self._state != _State.STRING or not self._string_pending:
            return
        self._string_value += ''.join(self._string_pending)
        self._string_pending.clear()
        if self._string_is_key:
            return
        if self._string_parent is None:
            self.data = self._string_value
        elif self._string_parent.kind == 'object':
            self._string_parent.value[self._string_key] = self._string_value
        else:
            self._string_parent.value[self._string_index] = self._string_value

    def _unicode_char(self, unit: int) -> None:
        if self._high_surrogate is not None:
            if not 0xDC00 <= unit <= 0xDFFF:
                raise ValueError('Esperado surrogate baixo após surrogate alto')
            codepoint = 0x10000 + ((self._high_surrogate - 0xD800) << 10) + (unit - 0xDC00)
            self._high_surrogate = None
            self._string_pending.append(chr(codepoint))
        elif 0xD800 <= unit <= 0xDBFF:
            self._high_surrogate = unit
        elif 0xDC00 <= unit <= 0xDFFF:
            raise ValueError('Surrogate baixo sem surrogate alto')
        else:
            self._string_pending.append(chr(unit))

    def _consume_string(self, char: str) -> None:
        if self._unicode_digits is not None:
            if char not in '0123456789abcdefABCDEF':
                raise ValueError('Escape Unicode inválido')
            self._unicode_digits += char
            if len(self._unicode_digits) == 4:
                self._unicode_char(int(self._unicode_digits, 16))
                self._unicode_digits = None
            return

        if self._escaped:
            self._escaped = False
            if char == 'u':
                self._unicode_digits = ''
            elif char in _ESCAPES and self._high_surrogate is None:
                self._string_pending.append(_ESCAPES[char])
            else:
                raise ValueError('Escape inválido em string')
            return

        if char == '\\':
            self._escaped = True
            return

        if self._high_surrogate is not None:
            raise ValueError('Surrogate alto não seguido por escape Unicode')

        if char == '"':
            self._flush_string()
            if self._string_is_key:
                if self._string_value in self._frames[-1].value:
                    raise ValueError(f'Chave JSON duplicada: {self._string_value!r}')
                self._frames[-1].key = self._string_value
                self._state = _State.COLON
            else:
                self._completed.add(self._value_path)
                self._after_value()
            return

        if ord(char) < 32 or 0xD800 <= ord(char) <= 0xDFFF:
            raise ValueError('Caractere de controle ou surrogate isolado em string')
        self._string_pending.append(char)

    def _open(self, kind: str) -> None:
        value = {} if kind == 'object' else []
        path = self._next_path()
        self._assign(value)
        self._frames.append(_Frame(kind=kind, value=value, path=path))
        self._state = _State.KEY_OR_END if kind == 'object' else _State.VALUE

    def _close(self, char: str) -> None:
        if not self._frames:
            raise ValueError('Fechamento inesperado')
        frame = self._frames[-1]
        if char != ('}' if frame.kind == 'object' else ']'):
            raise ValueError('Fechamento incompatível')
        self._frames.pop()
        self._completed.add(frame.path)
        self._after_value()

    def _after_value(self) -> None:
        if self._frames:
            self._frames[-1].allow_end = False
            self._state = _State.COMMA_OR_END
        else:
            self._state = _State.DONE

    def _finish_number(self) -> None:
        if _NUMBER.fullmatch(self._text) is None:
            raise ValueError(f'Número inválido: {self._text!r}')
        self._assign(json.loads(self._text))
        self._completed.add(self._value_path)
        self._after_value()

    def _consume(self, char: str) -> None:
        while True:
            if self._state == _State.STRING:
                self._consume_string(char)
                return

            if self._state == _State.NUMBER:
                if char in '0123456789+-.eE':
                    self._text += char
                    return
                self._finish_number()
                continue  # processa o delimitador na nova fase

            if self._state == _State.LITERAL:
                self._text += char
                if not self._literal.startswith(self._text):
                    raise ValueError('Literal inválido')
                if self._text == self._literal:
                    self._assign(_LITERALS[self._literal])
                    self._completed.add(self._value_path)
                    self._after_value()
                return

            if char in ' \t\r\n':
                return

            if self._state == _State.DONE:
                raise ValueError('Conteúdo após JSON completo')

            if self._state == _State.KEY_OR_END:
                if char == '}':
                    if not self._frames[-1].allow_end:
                        raise ValueError('Vírgula final não permitida em objeto')
                    self._close(char)
                elif char == '"':
                    self._start_string(is_key=True)
                else:
                    raise ValueError('Esperada chave de objeto ou }')
                return

            if self._state == _State.COLON:
                if char != ':':
                    raise ValueError('Esperado : depois de chave')
                self._state = _State.VALUE
                return

            if self._state == _State.VALUE:
                if char == '{':
                    self._open('object')
                elif char == '[':
                    self._open('array')
                elif char == '"':
                    self._start_string(is_key=False)
                elif char in '-0123456789':
                    self._text = char
                    self._value_path = self._next_path()
                    self._state = _State.NUMBER
                elif char in 'tfn':
                    self._literal = {'t': 'true', 'f': 'false', 'n': 'null'}[char]
                    self._text = char
                    self._value_path = self._next_path()
                    self._state = _State.LITERAL
                    if self._text == self._literal:  # genérico, nunca acontece aqui
                        self._assign(_LITERALS[self._literal])
                        self._completed.add(self._value_path)
                        self._after_value()
                elif char == ']' and self._frames and self._frames[-1].kind == 'array':
                    if not self._frames[-1].allow_end:
                        raise ValueError('Vírgula final não permitida em array')
                    self._close(char)
                else:
                    raise ValueError(f'Valor JSON inesperado: {char!r}')
                return

            if self._state == _State.COMMA_OR_END:
                if char in '}]':
                    self._close(char)
                elif char == ',':
                    if not self._frames:
                        raise ValueError('Vírgula inesperada')
                    frame = self._frames[-1]
                    frame.allow_end = False
                    if frame.kind == 'object':
                        frame.key = None
                        self._state = _State.KEY_OR_END
                    else:
                        self._state = _State.VALUE
                else:
                    raise ValueError('Esperada vírgula ou fechamento')
                return

            raise RuntimeError(f'Estado não tratado: {self._state}')
