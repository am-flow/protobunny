import pypandoc


if __name__ == "__main__":
    # convert README.md to docs/source/intro.rst
    # pypandoc.convert_file("README.md", "rst", outputfile="docs/source/intro.rst")
    # convert QUICK_START.md to docs/source/quick_start.rst
    pypandoc.convert_file("QUICK_START.md", "rst", outputfile="docs/source/quick_start.rst")
