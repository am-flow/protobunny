import pypandoc


if __name__ == "__main__":
    pypandoc.convert_file("README.md", "rst", outputfile="docs/source/readme.rst")
    pypandoc.convert_file("QUICK_START.md", "rst", outputfile="docs/source/quick_start_rst.rst")
