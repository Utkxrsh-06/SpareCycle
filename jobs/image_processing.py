import io

from PIL import Image


ALLOWED_FORMATS = {"PNG", "JPEG", "WEBP"}


def process_image(data: bytes, operation: str, params: dict):
    with Image.open(io.BytesIO(data)) as image:
        if operation == "thumbnail":
            image.thumbnail(
                (
                    int(params.get("width", 256)),
                    int(params.get("height", 256)),
                )
            )
        elif operation == "resize":
            image = image.resize(
                (int(params["width"]), int(params["height"]))
            )
        elif operation != "convert":
            raise ValueError("unsupported image operation")
        fmt = str(params.get("format", image.format or "PNG")).upper()
        if fmt not in ALLOWED_FORMATS:
            raise ValueError("unsupported output format")
        if fmt == "JPEG" and image.mode not in ("RGB", "L"):
            image = image.convert("RGB")
        output = io.BytesIO()
        image.save(output, format=fmt)
        return output.getvalue(), fmt.lower()
