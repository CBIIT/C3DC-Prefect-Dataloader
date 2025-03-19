from prefect import flow, task
import os
import sys

sys.path.insert(0, os.path.abspath("./icdc-dataloader"))
from loader import main
from config import PluginConfig
from bento.common.secret_manager import get_secret
import boto3
import json
from botocore.exceptions import ClientError
from typing import Literal
from datetime import datetime
from pytz import timezone
import pkg_resources
import inflect
import yaml

NEO4J_URI = "neo4j_uri"
NEO4J_PASSWORD = "neo4j_password"
SUBMISSION_BUCKET = "submission_bucket"

DropDownChoices = Literal[True, False]
ModeDropDownChoices = Literal["upsert", "new", "delete"]


def get_time() -> str:
    """Returns the current time"""
    tz = timezone("EST")
    now = datetime.now(tz)
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string


@task(log_prints=True)
def create_prop_file(
    model_yaml: str, delimiter: str, domain_value: str = "Unknown.domain.nci.nih.gov"
) -> str:
    """Create a prop file based on the model yaml

    Args:
        model_yaml (str): Filepath of the model yaml
        delimiter (str): delimiter for this project
        domain_value (str): domain value for this project. Defaults to "Unknown.domain.nci.nih.gov".

    Returns:
        str: Filepath of the prop file  
    """
    return_dict = {}
    return_dict["Properties"] = {}
    return_dict["Properties"]["domain_value"] = domain_value
    return_dict["Properties"]["rel_prop_delimiter"] = "$"
    return_dict["Properties"]["delimiter"] = delimiter
    with open(model_yaml, "r") as model:
        model_dict = yaml.safe_load(model)
    # wanna make sure it was using 1.4.0
    print(json.dumps(model_dict, indent=4))
    node_list =  list(model_dict["Nodes"].keys())
    plural_dict = {}
    id_dict = {}
    plural_engine = inflect.engine()
    for node in node_list:
        if "_"in node:
            last_word = node.split("_")[-1]
            last_word_plural = plural_engine.plural(last_word)
            node_name_list =  node.split("_")[:-1] + [last_word_plural]
            node_plural = "_".join(node_name_list)
        else:
            node_plural = plural_engine.plural(node)
        plural_dict[node] = node_plural
        id_dict[node] = "id"
    return_dict["Properties"]["plurals"] = plural_dict
    return_dict["Properties"]["type_mapping"] = {
        "string": "String",
        "number": "Float",
        "integer": "Int",
        "boolean": "Boolean",
        "array": "Array",
        "object": "Object",
        "datetime": "DateTime",
        "date": "Date",
        "TBD": "String",
    }
    return_dict["Properties"]["id_fields"] = id_dict
    prop_file_name = "props_file.yaml"
    with open(prop_file_name, "w") as prop_file:
        yaml.dump(return_dict, prop_file, sort_keys=False)

    # print yaml file for checking
    print("Print the content of props_file.yaml")
    f = open(prop_file_name, "r")
    file_contents = f.read()
    print(file_contents)
    f.close()
    return prop_file_name


def load_data(
        s3_bucket,
        s3_folder,
        upload_log_dir = None,
        dataset = "data",
        temp_folder = "tmp",
        uri = "bolt://127.0.0.1:7687",
        user = "neo4j",
        password = "123456",
        schemas = ["../ccdi-model/model-desc/icdc-model.yml", "../ccdi-model/model-desc/icdc-model-props.yml"],
        prop_file = "./icdc-dataloader/config/props-ccdi-model.yml",
        backup_folder = "tmp/data-loader-backups",
        cheat_mode = False,
        dry_run = False,
        wipe_db = False,
        no_backup = True,
        no_parents = True,
        verbose = False,
        yes = True,
        max_violation = 1000000,
        mode = "upsert",
        split_transaction = False,
        plugins = []
    ) -> None:

    params = Config(
        dataset,
        uri,
        user,
        password,
        schemas,
        prop_file,
        s3_bucket,
        s3_folder,
        backup_folder,
        cheat_mode,
        dry_run,
        wipe_db,
        no_backup,
        no_parents,
        verbose,
        yes,
        max_violation,
        mode,
        split_transaction,
        upload_log_dir,
        plugins,
        temp_folder
    )
    main(params)
    return None

class Config:
    def __init__(
            self,
            dataset,
            uri,
            user,
            password,
            schemas,
            prop_file,
            bucket,
            s3_folder,
            backup_folder,
            cheat_mode,
            dry_run,
            wipe_db,
            no_backup,
            no_parents,
            verbose,
            yes,
            max_violation,
            mode,
            split_transaction,
            upload_log_dir,
            plugins,
            temp_folder
    ):
        self.dataset = dataset
        self.uri = uri
        self.user = user
        self.password = password
        self.schema = schemas
        self.prop_file = prop_file
        self.bucket = bucket
        self.s3_folder = s3_folder
        self.backup_folder = backup_folder
        self.cheat_mode = cheat_mode
        self.dry_run = dry_run
        self.wipe_db = wipe_db
        self.no_backup = no_backup
        self.no_parents = no_parents
        self.verbose = verbose
        self.yes = yes
        self.max_violations = max_violation
        self.mode = mode
        self.split_transactions = split_transaction
        self.upload_log_dir = upload_log_dir
        self.plugins = []
        self.temp_folder = temp_folder
        for plugin in plugins:
            self.plugins.append(PluginConfig(plugin))

        self.config_file = None


@flow(name="C3DC Data Loader", log_prints=True)
def c3dc_hub_data_loader(
        secret_name: str,
        metadata_folder: str,
        runner: str,
        model_tag: str,
        cheat_mode: DropDownChoices,
        dry_run: DropDownChoices,
        wipe_db: DropDownChoices,
        mode: ModeDropDownChoices,
        split_transaction: DropDownChoices,
    ) -> None: 
    """Entrypoint of prefect data loader for C3DC DEV tier DB

    Args:
        secret_name (str): secret name stored in AWS secrets manager.
        metadata_folder (str): folder name under bucket s3://c3dc-curation.
        runner (str): unique runner name that will be used for log folder
        model_tag (str): tag of the model to use.
        cheat_mode (DropDownChoices): If turn on cheat mode.
        dry_run (DropDownChoices): if dry run.
        wipe_db (DropDownChoices): if wipe the entire database.
        mode (ModeDropDownChoices): data loading mode.
        split_transaction (DropDownChoices): if split transaction.
    """
    print("Getting secrets from AWS Secrets Manager")
    secret = get_secret(secret_name)
    uri = secret[NEO4J_URI]
    password = secret[NEO4J_PASSWORD]
    s3_bucket = secret[SUBMISSION_BUCKET]
    if not metadata_folder.endswith("/"):
        metadata_folder= metadata_folder + "/"
    else:
        pass
    s3_folder = f'{metadata_folder}'

    log_folder = f"prefect_c3dc_dataloader_{get_time()}"
    if runner.endswith("/"):
        runner= runner[:-1]
    else:
        pass
    upload_log_dir = f's3://{s3_bucket}/{runner}/{log_folder}/logs'

    schemas = [
        f"../c3dc-model/model-desc/c3dc-model.yml",
        f"../c3dc-model/model-desc/c3dc-model-props.yml",
    ]
    # hard coded value for C3DC
    domain_value = "clinicalcommons.ccdi.cancer.gov"
    metadata_delimiter = ";"
    prop_file = create_prop_file(model_yaml=schemas[0], delimiter=metadata_delimiter, domain_value=domain_value)

    print("start loading data")
    # os.mkdir("data")
    # os.mkdir("tmp")
    print(os.listdir(".."))
    print(os.listdir("."))
    load_data(
        s3_bucket=s3_bucket,
        s3_folder=s3_folder,
        upload_log_dir=upload_log_dir,
        dataset="data",
        temp_folder="tmp",
        uri=uri,
        password=password,
        schemas=schemas,
        prop_file=prop_file,
        cheat_mode=cheat_mode,
        dry_run=dry_run,
        wipe_db=wipe_db,
        no_backup=True, # turn off backup as default
        yes=True, # default as True
        max_violation=1000000, # default max violation to 1,000,000
        mode=mode,
        split_transaction=split_transaction,
        plugins=[], # default as empty list
    )
    print(f"log file can be found in the s3 location {upload_log_dir}")     

    return None

if __name__ == "__main__":
    # create your first deployment
    load_data.serve(name="local-data-loader-deployment")
