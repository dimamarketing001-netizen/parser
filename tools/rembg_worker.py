"""Удаление фона для Студии (2026-11-20). Запускается ОТДЕЛЬНЫМ окружением ~/ParsYa/.venv-rembg,
чтобы зависимости rembg/onnxruntime не трогали окружение parsya-control.
stdin: картинка (PNG/JPEG/WebP), argv[1]: модель (по умолчанию isnet-general-use).
stdout: PNG с прозрачным фоном, обрезанный по объекту (+ небольшой отступ)."""
import io
import sys

from PIL import Image
from rembg import new_session, remove


def main() -> None:
    model = sys.argv[1] if len(sys.argv) > 1 else "isnet-general-use"
    img = Image.open(io.BytesIO(sys.stdin.buffer.read())).convert("RGBA")
    out = remove(img, session=new_session(model))
    box = out.getchannel("A").point(lambda a: 255 if a > 8 else 0).getbbox()
    if box:
        pad = int(max(out.size) * 0.02)
        out = out.crop((max(0, box[0] - pad), max(0, box[1] - pad), min(out.width, box[2] + pad), min(out.height, box[3] + pad)))
    buf = io.BytesIO()
    out.save(buf, "PNG", optimize=True)
    sys.stdout.buffer.write(buf.getvalue())


if __name__ == "__main__":
    main()
