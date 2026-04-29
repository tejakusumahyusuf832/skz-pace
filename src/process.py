import time

from tqdm import tqdm
import typer

app = typer.Typer()


def set_process(start: int = 0, end: int = 10):

    for i in tqdm(range(start, end + 1), desc=f"Processing from {start} to {end}..."):
        tqdm.write(str(i))

        time.sleep(1)


@app.command()
def main(
    start: int = typer.Option(0, help="Starting number."),
    end: int = typer.Option(10, help="End number."),
):

    set_process(start=start, end=end)


if __name__ == "__main__":
    app()
