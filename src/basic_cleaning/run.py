#!/usr/bin/env python
"""
Downloads raw dataset from W&B, applies basic cleaninsing and exports to a new artiwew artifact
"""
import argparse
import logging
import wandb
import pandas as pd
import tempfile
from wandb_utils.log_artifact import log_artifact
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f"Fetching artifact {args.input_artifact}")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Artifact retrieved, reading it locally.")
    df = pd.read_csv(artifact_local_path)

    logger.info("Data read. Starting cleansing.")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info(f"Data cleansed. Saving and uploading {args.output_artifact} artifact.")
    with open(args.output_artifact, "w") as fp:
        df.to_csv(fp.name, index=False)
        log_artifact(
            args.output_artifact,
            args.output_type,
            args.output_description,
            fp.name,
            run
        )
    
    logger.info("Cleaned data artifact registered. Tidying up temp files.")
    os.remove(args.output_artifact) #tidying up
    logger.info("DONE - CLEANING")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type = str,
        help= "Raw data artifact to be cleaned",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type= str,
        help= "Name of cleaned data artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type= str,
        help= "Type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type= str,
        help= "Description of output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type= float,
        help= "Min allowed price for outliear removal",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type= float,
        help= "Max allowed price for outliear removal",
        required=True
    )


    args = parser.parse_args()

    go(args)
