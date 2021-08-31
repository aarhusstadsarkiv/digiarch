from os import read
from pathlib import Path


def is_binary(file: Path) -> bool:
    bytes_of_file = file.read_bytes()
    if b'\x00' in bytes_of_file:
        return True
    else:
        return False

if __name__ == "__main__":
    path_to_txt = Path('C:\\Users\\az58999\\digiarch\\digiarch\\core\\text_file.txt')

    path_to_binary = Path('D:\\rel_path_test\\AVID.AARS.53.1\\docCollection1\\874\Årsrapport 2009.pdf')

    text = is_binary(path_to_txt)

    binary = is_binary(path_to_binary)

    if text:
        print("The file text file is binary which is not correct.")
    
    if binary:
        print("The binary file is binary, which is correct.")