from pathlib import Path
import re

from loguru import logger
from tqdm import tqdm
import typer

from src.config import PROCESSED_DATA_DIR

app = typer.Typer()


def categorize_content(title: str) -> str:
    """Categorize video content based on regex pattern matching against the title.

    Args:
        title (str): The title of the YouTube video.

    Returns:
        str: The designated content category. Returns "Other/Misc" if no
            specific pattern is matched.
    """
    title_upper = title.upper()

    if re.search(r"SKZ CODE|SKZCODE", title_upper):
        return "SKZ CODE"
    elif re.search(r"2 KIDS ROOM|TWO KIDS ROOM", title_upper):
        return "2 Kids Room"
    elif re.search(r"1 KIDS ROOM", title_upper):
        return "1 Kids Room"
    elif re.search(r"SKZ-TALKER|SKZ TALKER", title_upper):
        return "SKZ-TALKER"
    elif re.search(r"SKZ-PLAYER|SKZ-RECORD", title_upper):
        return "SKZ-PLAYER/RECORD"
    elif re.search(r"UNVEIL : TRACK|M/V TEASER|TRAILER", title_upper):
        return "Teaser/Trailer"
    elif re.search(r"M/V$", title_upper) or re.search(r"MUSIC VIDEO", title_upper):
        return "Official M/V"
    elif re.search(r"VLOG", title_upper):
        return "Vlog"
    else:
        return "Other/Misc"


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
    output_path: Path = PROCESSED_DATA_DIR / "features.csv",
    # -----------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Generating features from dataset...")
    for i in tqdm(range(10), total=10):
        if i == 5:
            logger.info("Something happened for iteration 5.")
    logger.success("Features generation complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
