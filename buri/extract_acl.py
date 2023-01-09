from argparse import ArgumentParser

import pandas as pd
import sienna

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--data-path",
        type=str,
        required=True,
        help="Paht to `acl-publication-info.74k.parquet` file.",
    )
    parser.add_argument(
        "--opath",
        type=str,
        required=True,
        help="Path to save a line separated big file.",
    )
    parser.add_argument(
        "--year-until",
        type=int,
        required=False,
        default=10000,
        help="To remove new papers.",
    )
    args = parser.parse_args()

    df = pd.read_parquet(args.data_path)
    print(f"{len(df)} rows in original data.")

    # Remove rows with empty title, abstract or full_text
    df = df[
        (df.title.str.len() > 0)
        & (df.abstract.str.len() > 0)
        & (df.full_text.str.len() > 0)
    ]
    print(f"{len(df)} rows after removing empty texts.")

    # Year limitation
    df = df[df.year.astype(int) <= args.year_until]
    print(f"{len(df)} rows after limiting by year (= {args.year_until}).")

    # Replace "\n" with space
    # Append title, abstract and full_text
    texts = (
        df.title.str.replace("\n", " ")
        + ". "
        + df.abstract.str.replace("\n", " ")
        + " "
        + df.full_text.str.replace("\n", " ")
    ).values.tolist()

    # Save as one gigantic line separated file.
    print(f"Saving to {args.opath}...")
    sienna.save(texts, args.opath)
