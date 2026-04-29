import typer

from src.process import set_process

app = typer.Typer()


@app.command()
def main(
    number: int = typer.Option(10, help="Insert any integer to process."),
):

    set_process(end=number)


if __name__ == "__main__":
    app()


# if __name__ == "__main__":

#     typer.run(main)
