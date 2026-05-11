from __future__ import annotations

import html


_DATA_BLOCKS_L: dict[int, list[int]] = {
    1: [19],
    2: [34],
    3: [55],
    4: [80],
    5: [108],
    6: [68, 68],
}
_ECC_WORDS_L = {1: 7, 2: 10, 3: 15, 4: 20, 5: 26, 6: 18}
_ALIGN_CENTERS = {1: [], 2: [6, 18], 3: [6, 22], 4: [6, 26], 5: [6, 30], 6: [6, 34]}


def qr_svg(text: str, *, scale: int = 6, border: int = 4) -> str:
    payload = text.encode("utf-8")
    version = _select_version(len(payload))
    matrix = _build_matrix(payload, version)
    n = len(matrix)
    size = (n + border * 2) * scale
    paths: list[str] = []
    for y, row in enumerate(matrix):
        for x, dark in enumerate(row):
            if dark:
                paths.append(f"M{(x + border) * scale},{(y + border) * scale}h{scale}v{scale}h-{scale}z")
    title = html.escape("Authenticator setup QR")
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" '
        f'width="{size}" height="{size}" role="img" aria-label="{title}">'
        f'<rect width="100%" height="100%" fill="#fff"/>'
        f'<path fill="#000" d="{"".join(paths)}"/></svg>'
    )


def _select_version(byte_len: int) -> int:
    for version, blocks in _DATA_BLOCKS_L.items():
        capacity = sum(blocks)
        needed = 2 + byte_len
        if needed <= capacity:
            return version
    raise ValueError("qr payload too large")


def _build_matrix(payload: bytes, version: int) -> list[list[bool]]:
    data = _encode_data(payload, version)
    ecc = _add_ecc(data, version)
    size = 21 + 4 * (version - 1)
    base = [[False for _ in range(size)] for _ in range(size)]
    reserved = [[False for _ in range(size)] for _ in range(size)]
    _draw_function_patterns(base, reserved, version)
    _place_codewords(base, reserved, ecc)
    best = None
    best_score = 10**9
    for mask in range(8):
        candidate = [row[:] for row in base]
        _apply_mask(candidate, reserved, mask)
        _draw_format(candidate, mask)
        score = _penalty(candidate)
        if score < best_score:
            best = candidate
            best_score = score
    return best if best is not None else base


def _encode_data(payload: bytes, version: int) -> list[int]:
    capacity = sum(_DATA_BLOCKS_L[version]) * 8
    bits: list[int] = []
    _append_bits(bits, 0b0100, 4)
    _append_bits(bits, len(payload), 8)
    for byte in payload:
        _append_bits(bits, byte, 8)
    bits.extend([0] * min(4, capacity - len(bits)))
    while len(bits) % 8:
        bits.append(0)
    pads = (0xEC, 0x11)
    i = 0
    while len(bits) < capacity:
        _append_bits(bits, pads[i % 2], 8)
        i += 1
    return [int("".join(str(bit) for bit in bits[i : i + 8]), 2) for i in range(0, len(bits), 8)]


def _append_bits(bits: list[int], value: int, count: int) -> None:
    for i in range(count - 1, -1, -1):
        bits.append((value >> i) & 1)


def _add_ecc(data: list[int], version: int) -> list[int]:
    sizes = _DATA_BLOCKS_L[version]
    ecc_len = _ECC_WORDS_L[version]
    blocks: list[list[int]] = []
    pos = 0
    for size in sizes:
        block = data[pos : pos + size]
        pos += size
        blocks.append(block)
    ecc_blocks = [_rs_remainder(block, ecc_len) for block in blocks]
    out: list[int] = []
    for i in range(max(len(block) for block in blocks)):
        for block in blocks:
            if i < len(block):
                out.append(block[i])
    for i in range(ecc_len):
        for block in ecc_blocks:
            out.append(block[i])
    return out


def _rs_remainder(data: list[int], degree: int) -> list[int]:
    gen = _rs_generator(degree)
    rem = [0] * degree
    for byte in data:
        factor = byte ^ rem.pop(0)
        rem.append(0)
        if factor:
            for i, coef in enumerate(gen):
                rem[i] ^= _gf_mul(coef, factor)
    return rem


def _rs_generator(degree: int) -> list[int]:
    poly = [1]
    for i in range(degree):
        poly = _poly_mul(poly, [1, _gf_pow(2, i)])
    return poly[1:]


def _poly_mul(a: list[int], b: list[int]) -> list[int]:
    out = [0] * (len(a) + len(b) - 1)
    for i, x in enumerate(a):
        for j, y in enumerate(b):
            out[i + j] ^= _gf_mul(x, y)
    return out


def _gf_mul(x: int, y: int) -> int:
    z = 0
    for i in range(8):
        if (y >> i) & 1:
            z ^= x << i
    for i in range(14, 7, -1):
        if (z >> i) & 1:
            z ^= 0x11D << (i - 8)
    return z & 0xFF


def _gf_pow(x: int, power: int) -> int:
    out = 1
    for _ in range(power):
        out = _gf_mul(out, x)
    return out


def _draw_function_patterns(m: list[list[bool]], r: list[list[bool]], version: int) -> None:
    size = len(m)
    for x, y in ((0, 0), (size - 7, 0), (0, size - 7)):
        _draw_finder(m, r, x, y)
    for i in range(8, size - 8):
        _set(m, r, i, 6, i % 2 == 0)
        _set(m, r, 6, i, i % 2 == 0)
    for cy in _ALIGN_CENTERS[version]:
        for cx in _ALIGN_CENTERS[version]:
            if (cx <= 8 and cy <= 8) or (cx >= size - 9 and cy <= 8) or (cx <= 8 and cy >= size - 9):
                continue
            _draw_alignment(m, r, cx, cy)
    _set(m, r, 8, size - 8, True)
    for i in range(9):
        r[8][i] = True
        r[i][8] = True
    for i in range(size - 8, size):
        r[8][i] = True
        r[i][8] = True


def _draw_finder(m: list[list[bool]], r: list[list[bool]], x: int, y: int) -> None:
    for dy in range(-1, 8):
        for dx in range(-1, 8):
            xx, yy = x + dx, y + dy
            if 0 <= xx < len(m) and 0 <= yy < len(m):
                dark = 0 <= dx <= 6 and 0 <= dy <= 6 and (dx in {0, 6} or dy in {0, 6} or (2 <= dx <= 4 and 2 <= dy <= 4))
                _set(m, r, xx, yy, dark)


def _draw_alignment(m: list[list[bool]], r: list[list[bool]], cx: int, cy: int) -> None:
    for dy in range(-2, 3):
        for dx in range(-2, 3):
            _set(m, r, cx + dx, cy + dy, max(abs(dx), abs(dy)) != 1)


def _set(m: list[list[bool]], r: list[list[bool]], x: int, y: int, value: bool) -> None:
    m[y][x] = value
    r[y][x] = True


def _place_codewords(m: list[list[bool]], r: list[list[bool]], codewords: list[int]) -> None:
    bits = [(byte >> i) & 1 for byte in codewords for i in range(7, -1, -1)]
    size = len(m)
    idx = 0
    upward = True
    x = size - 1
    while x > 0:
        if x == 6:
            x -= 1
        for _ in range(size):
            y = (size - 1 - _) if upward else _
            for xx in (x, x - 1):
                if not r[y][xx]:
                    m[y][xx] = bool(bits[idx]) if idx < len(bits) else False
                    idx += 1
        upward = not upward
        x -= 2


def _apply_mask(m: list[list[bool]], r: list[list[bool]], mask: int) -> None:
    for y in range(len(m)):
        for x in range(len(m)):
            if not r[y][x] and _mask_bit(mask, x, y):
                m[y][x] = not m[y][x]


def _mask_bit(mask: int, x: int, y: int) -> bool:
    return (
        (x + y) % 2 == 0,
        y % 2 == 0,
        x % 3 == 0,
        (x + y) % 3 == 0,
        (x // 3 + y // 2) % 2 == 0,
        (x * y) % 2 + (x * y) % 3 == 0,
        ((x * y) % 2 + (x * y) % 3) % 2 == 0,
        ((x + y) % 2 + (x * y) % 3) % 2 == 0,
    )[mask]


def _draw_format(m: list[list[bool]], mask: int) -> None:
    size = len(m)
    value = _format_bits(mask)
    coords1 = [(8, i) for i in range(6)] + [(8, 7), (8, 8), (7, 8)] + [(i, 8) for i in range(5, -1, -1)]
    coords2 = [(size - 1 - i, 8) for i in range(8)] + [(8, size - 7 + i) for i in range(7)]
    for i, (x, y) in enumerate(coords1):
        m[y][x] = bool((value >> i) & 1)
    for i, (x, y) in enumerate(coords2):
        m[y][x] = bool((value >> i) & 1)


def _format_bits(mask: int) -> int:
    data = (0b01 << 3) | mask
    value = data << 10
    gen = 0b10100110111
    for i in range(14, 9, -1):
        if (value >> i) & 1:
            value ^= gen << (i - 10)
    return ((data << 10) | value) ^ 0b101010000010010


def _penalty(m: list[list[bool]]) -> int:
    size = len(m)
    score = 0
    for rows in (m, [[m[y][x] for y in range(size)] for x in range(size)]):
        for row in rows:
            run_color = row[0]
            run = 1
            for bit in row[1:]:
                if bit == run_color:
                    run += 1
                else:
                    if run >= 5:
                        score += run - 2
                    run_color = bit
                    run = 1
            if run >= 5:
                score += run - 2
            for i in range(size - 10):
                if row[i : i + 11] in ([True, False, True, True, True, False, True, False, False, False, False], [False, False, False, False, True, False, True, True, True, False, True]):
                    score += 40
    for y in range(size - 1):
        for x in range(size - 1):
            if m[y][x] == m[y][x + 1] == m[y + 1][x] == m[y + 1][x + 1]:
                score += 3
    dark = sum(1 for row in m for bit in row if bit)
    score += abs(dark * 20 // (size * size) - 10) * 10
    return score
