from pipeline import step1_preprocess, step2_tagging, step3_enrichment, step4_brand_extraction
from pathlib import Path

def main():
    raw_path = Path("data/raw/input.xlsx")
    stage1_path = Path("data/stage1_cleaned/cleaned.parquet")
    stage2_path = Path("data/stage2_enriched/enriched.parquet")
    stage3_path = Path("data/stage3_tagged/tagged.parquet")
    final_path = Path("data/final/output.xlsx")

    # Step 1: Preprocess raw input
    if not stage1_path.exists():
        step1_preprocess.run(raw_path, stage1_path)

    # Step 2: Enrichment from external sources
    if not stage2_path.exists():
        step3_enrichment.run(stage1_path, stage2_path)

    # Step 3: Tagging with fuzzy logic
    if not stage3_path.exists():
        step2_tagging.run(stage2_path, stage3_path)

    # Step 4: Final export
    step4_brand_extraction.run(stage3_path, final_path)

if __name__ == "__main__":
    main()
