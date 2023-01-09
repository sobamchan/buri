import gzip
import json
import os
from argparse import ArgumentParser, Namespace

import sienna
from tqdm import tqdm


def with_title(args: Namespace):
    # Extract titles
    print("Extract titiles...")
    meta_data_dir = os.path.join(args.data_dir, "metadata")
    for fname in tqdm(os.listdir(meta_data_dir)):
        fpath = os.path.join(meta_data_dir, fname)

        with gzip.open(fpath) as fp:
            for line in fp:
                metadata = json.loads(line.decode("utf-8"))
                paper_id = metadata["paper_id"]
                title = metadata["title"]
                abstract = metadata["abstract"]
                sienna.save(
                    [{"paper_id": paper_id, "title": title, "abstract": abstract}],
                    os.path.join(args.odir, f"{paper_id}.jsonl"),
                )

    # Extract main texts and add file created above
    print("Extract body texts...")
    pdf_parses_path = os.path.join(args.data_dir, "pdf_parses")
    for fname in os.listdir(pdf_parses_path):
        fpath = os.path.join(pdf_parses_path, fname)
        with gzip.open(fpath) as fp:
            for line in fp:
                pdfparses = json.loads(line.decode("utf-8"))
                paper_id = pdfparses["paper_id"]
                text = [
                    f"{body_text['section']} body_text['text']"
                    for body_text in pdfparses["body_text"]
                ]
                record = sienna.load(os.path.join(args.odir, f"{paper_id}.jsonl"))[0]
                record["body_text"] = text

                if args.final_single_file is not None:
                    with open(args.final_single_file, "a") as f:
                        f.write(json.dumps(record) + "\n")


def without_title(args):
    assert isinstance(
        args.final_single_file, str
    ), "This function now only supports to write to one huge file."

    final_fp = open(args.final_single_file, "a")

    # Extract main texts and add file created above
    print("Extract body texts...")
    pdf_parses_path = os.path.join(args.data_dir, "pdf_parses")
    for fname in tqdm(os.listdir(pdf_parses_path)):
        fpath = os.path.join(pdf_parses_path, fname)
        with gzip.open(fpath) as fp:
            for line in fp:
                pdfparse = json.loads(line.decode("utf-8"))
                texts = [
                    f"{section['section']}. {section['text']}"
                    for section in pdfparse["abstract"] + pdfparse["body_text"]
                    if section["text"] != ""
                ]
                if len(texts) == 0:
                    pass
                else:
                    final_fp.write(" ".join(texts).replace("\n", " ") + "\n")

    final_fp.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Path to S2ORC dataset dir. It should contain two dirs, `metadata` and `pdf_parses`",
    )
    parser.add_argument(
        "--odir", type=str, required=True, help="Path to save generated text file."
    )
    parser.add_argument(
        "--final-single-file",
        type=str,
        required=False,
        default=None,
        help="One large jsonl file contains all records.",
    )
    parser.add_argument("--with-title", action="store_true")
    args = parser.parse_args()

    if args.with_title:
        with_title(args)
    else:
        without_title(args)
