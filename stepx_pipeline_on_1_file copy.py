import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent / "pipeline"))

from pipeline.step3_enrichment import unify_country_names
from pipeline.utils.io import read_excel_file, save_to_excel_file

def run_single_file():
    df = read_excel_file("data/st6_datamart/st6.xlsx")
    df = unify_country_names(df, ["prod_coo", "exporter_country", "importer_country"])
    Path("data/st1_cleaned").mkdir(parents=True, exist_ok=True)
    save_to_excel_file(df, "data/st6_datamart/st6.xlsx")


if __name__ == "__main__":
    run_single_file()
