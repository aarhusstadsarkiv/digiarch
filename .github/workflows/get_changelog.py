from sys import argv


def main(file: str, tag: str) -> str:
    header: str = f"## v{tag.lstrip('v')}"
    body: list[str] = []
    is_body: bool = False
    with open(file, "r", encoding="utf-8") as f:
        while line := f.readline():
            line = line.rstrip()
            if is_body and line.startswith("## "):
                break
            elif line == header or line.startswith(f"{header} "):
                is_body = True
                continue
            elif is_body and line.startswith("#"):
                body.append(line[1:])
            elif is_body:
                body.append(line)

    return "\n".join(body).strip()


if __name__ == "__main__":
    print(main(argv[1], argv[2]), end="", flush=True)
