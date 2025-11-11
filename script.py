"""Script de publicação inspirado no script_dataverse.py.

Cria (se necessário) um Dataverse filho, gera um dataset a partir de um template
JSON e envia todos os arquivos produzidos pelo workflow para o Dataverse alvo.
Todo o comportamento pode ser configurado via variáveis de ambiente ou
argumentos de linha de comando para permitir o uso dentro do AkôFlow.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable

from pyDataverse.api import NativeApi
from pyDataverse.models import Datafile, Dataset, Dataverse

DATASET_FILENAME = "dataset.json"
DATASET_SAMPLE_FILENAME = "dataset.sample.json"


def default_dataset_template_path() -> str:
    """Resolve o caminho padrão do template de dataset."""
    base_dir = Path(__file__).parent
    dataset_path = base_dir / DATASET_FILENAME
    if dataset_path.is_file():
        return str(dataset_path)
    sample_path = base_dir / DATASET_SAMPLE_FILENAME
    if sample_path.is_file():
        return str(sample_path)
    # Retorna o caminho esperado para facilitar mensagens de erro posteriores.
    return str(dataset_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publica arquivos do workflow no Dataverse.")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("DATAVERSE_BASE_URL", "https://demo.dataverse.org/"),
        help="URL base do Dataverse (ex.: https://demo.dataverse.org/).",
    )
    parser.add_argument(
        "--api-token",
        default=os.environ.get("DATAVERSE_API_TOKEN"),
        required=os.environ.get("DATAVERSE_API_TOKEN") is None,
        help="Token de API do Dataverse.",
    )
    parser.add_argument(
        "--parent-dataverse",
        default=os.environ.get("DATAVERSE_PARENT_ALIAS"),
        required=os.environ.get("DATAVERSE_PARENT_ALIAS") is None,
        help="Alias do Dataverse pai onde o novo Dataverse/dataset será criado.",
    )
    parser.add_argument(
        "--dataverse-alias",
        default=os.environ.get("DATAVERSE_ALIAS")
        or os.environ.get("DATAVERSE_PARENT_ALIAS"),
        help="Alias do Dataverse filho (ex.: Dv14). Se omitido, usa o Dataverse pai.",
    )
    parser.add_argument(
        "--dataverse-name",
        default=os.environ.get("DATAVERSE_NAME", "Workflow Dataverse"),
        help="Nome amigável do Dataverse filho a ser criado (se não existir).",
    )
    parser.add_argument(
        "--dataverse-email",
        default=os.environ.get("DATAVERSE_CONTACT_EMAIL", "contato@example.com"),
        help="Email de contato que será associado ao Dataverse filho.",
    )
    parser.add_argument(
        "--dataverse-affiliation",
        default=os.environ.get("DATAVERSE_AFFILIATION", "AkôFlow"),
        help="Afiliacão exibida no Dataverse filho.",
    )
    parser.add_argument(
        "--dataverse-description",
        default=os.environ.get("DATAVERSE_DESCRIPTION", "Dataverse criado automaticamente pelo AkôFlow."),
        help="Descrição do Dataverse filho.",
    )
    parser.add_argument(
        "--dataset-json",
        default=os.environ.get("DATASET_JSON_PATH") or default_dataset_template_path(),
        help="Caminho para o template JSON do dataset.",
    )
    parser.add_argument(
        "--data-root",
        default=os.environ.get("DATA_ROOT", "/data"),
        help="Diretório raiz com os arquivos produzidos pelo workflow.",
    )
    parser.add_argument(
        "--title-suffix",
        default=os.environ.get("DATASET_TITLE_SUFFIX"),
        help="Sufixo opcional para incluir no título do dataset a fim de diferenciá-lo.",
    )
    parser.add_argument(
        "--skip-dataverse-creation",
        action="store_true",
        help="Não tenta criar o Dataverse filho (assume que já existe).",
    )
    parser.add_argument(
        "--citation-title",
        default=os.environ.get("DATASET_TITLE"),
        help="Título base do dataset (antes do sufixo).",
    )
    parser.add_argument(
        "--author-name",
        default=os.environ.get("DATASET_AUTHOR_NAME"),
        help="Nome do autor principal descrito no metadata.",
    )
    parser.add_argument(
        "--author-affiliation",
        default=os.environ.get("DATASET_AUTHOR_AFFILIATION"),
        help="Afiliacão do autor.",
    )
    parser.add_argument(
        "--author-identifier",
        default=os.environ.get("DATASET_AUTHOR_IDENTIFIER"),
        help="Identificador (ex.: ORCID) do autor.",
    )
    parser.add_argument(
        "--author-identifier-scheme",
        default=os.environ.get("DATASET_AUTHOR_IDENTIFIER_SCHEME"),
        help="Scheme do identificador (ex.: ORCID).",
    )
    parser.add_argument(
        "--contact-name",
        default=os.environ.get("DATASET_CONTACT_NAME"),
        help="Nome do contato do dataset.",
    )
    parser.add_argument(
        "--contact-email",
        default=os.environ.get("DATASET_CONTACT_EMAIL"),
        help="Email do contato do dataset.",
    )
    parser.add_argument(
        "--dataset-description",
        default=os.environ.get("DATASET_DESCRIPTION"),
        help="Descrição do dataset (campo dsDescriptionValue).",
    )
    return parser.parse_args()


def ensure_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else f"{url}/"


def load_dataset_template(path: Path) -> Dict:
    if not path.is_file():
        raise FileNotFoundError(f"Template de dataset não encontrado em {path}")
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def get_citation_fields(metadata: Dict) -> list[Dict]:
    return (
        metadata.setdefault("datasetVersion", {})
        .setdefault("metadataBlocks", {})
        .setdefault("citation", {})
        .setdefault("fields", [])
    )


def find_field(fields: list[Dict], type_name: str) -> Dict | None:
    return next((field for field in fields if field.get("typeName") == type_name), None)


def set_simple_field(fields: list[Dict], type_name: str, value: str) -> None:
    if value is None:
        return
    field = find_field(fields, type_name)
    if field is None:
        field = {
            "typeName": type_name,
            "multiple": False,
            "typeClass": "primitive",
            "value": value,
        }
        fields.append(field)
    else:
        field["value"] = value


def ensure_compound_entry(fields: list[Dict], type_name: str) -> Dict:
    field = find_field(fields, type_name)
    if field is None:
        field = {
            "typeName": type_name,
            "multiple": True,
            "typeClass": "compound",
            "value": [{}],
        }
        fields.append(field)
    if not field.get("value"):
        field["value"] = [{}]
    return field["value"][0]


def update_compound_entry(entry: Dict, values: Dict[str, str]) -> None:
    for sub_name, new_value in values.items():
        if new_value is None:
            continue
        subfield = entry.get(sub_name)
        if subfield is None:
            entry[sub_name] = {
                "typeName": sub_name,
                "multiple": False,
                "typeClass": "primitive",
                "value": new_value,
            }
        else:
            subfield["value"] = new_value


def apply_title_suffix(metadata: Dict, suffix: str) -> None:
    if not suffix:
        return
    fields = get_citation_fields(metadata)
    field = find_field(fields, "title")
    if field and isinstance(field.get("value"), str):
        field["value"] = f"{field['value']} {suffix}"
    else:
        set_simple_field(fields, "title", suffix)


def apply_metadata_overrides(metadata: Dict, args: argparse.Namespace) -> None:
    fields = get_citation_fields(metadata)

    if args.citation_title:
        set_simple_field(fields, "title", args.citation_title)

    author_entry = ensure_compound_entry(fields, "author")
    update_compound_entry(
        author_entry,
        {
            "authorName": args.author_name,
            "authorAffiliation": args.author_affiliation,
            "authorIdentifier": args.author_identifier,
            "authorIdentifierScheme": args.author_identifier_scheme,
        },
    )

    contact_entry = ensure_compound_entry(fields, "datasetContact")
    update_compound_entry(
        contact_entry,
        {
            "datasetContactName": args.contact_name,
            "datasetContactEmail": args.contact_email,
        },
    )

    description_entry = ensure_compound_entry(fields, "dsDescription")
    update_compound_entry(
        description_entry,
        {"dsDescriptionValue": args.dataset_description},
    )


def ensure_dataverse_exists(
    api: NativeApi,
    parent_alias: str,
    alias: str,
    name: str,
    contact_email: str,
    affiliation: str,
    description: str,
    skip_creation: bool,
) -> None:
    if skip_creation:
        print(f"Pulando criação do Dataverse '{alias}'.")
        return

    existing = api.get_dataverse(alias)
    if existing.status_code == 200:
        print(f"Dataverse '{alias}' já existe, reutilizando.")
        return

    dataverse = Dataverse()
    dataverse.set(
        {
            "alias": alias,
            "name": name,
            "dataverseContacts": [{"contactEmail": contact_email}],
            "affiliation": affiliation,
            "description": description,
            "permissionRoot": False,
        }
    )

    response = api.create_dataverse(parent_alias, dataverse.json())
    if response.status_code == 201:
        print(f"Dataverse '{alias}' criado com sucesso sob '{parent_alias}'.")
        return

    if response.status_code == 400 and "already exists" in response.text.lower():
        print(f"Dataverse '{alias}' já existia (resposta 400), prosseguindo.")
        return

    raise RuntimeError(
        f"Falha ao criar Dataverse '{alias}' (status {response.status_code}): {response.text}"
    )


def build_dataset(dataset_json_path: Path, args: argparse.Namespace) -> Dataset:
    metadata = load_dataset_template(dataset_json_path)

    if args.title_suffix is None:
        suffix = datetime.now(timezone.utc).strftime("(%Y-%m-%d %H:%M UTC)")
    else:
        suffix = args.title_suffix

    apply_title_suffix(metadata, suffix)
    apply_metadata_overrides(metadata, args)

    dataset = Dataset()
    dataset.from_json(json.dumps(metadata))
    dataset.validate_json()
    return dataset


def iter_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        raise FileNotFoundError(f"Diretório de dados não encontrado: {root}")
    for file_path in root.rglob("*"):
        if file_path.is_file():
            yield file_path


def create_dataset(api: NativeApi, dataverse_alias: str, dataset: Dataset) -> str:
    response = api.create_dataset(dataverse_alias, dataset.json())
    if response.status_code != 201:
        raise RuntimeError(
            f"Falha ao criar dataset (status {response.status_code}): {response.text}"
        )

    dataset_pid = response.json()["data"]["persistentId"]
    print(f"Dataset criado com PID: {dataset_pid}")
    return dataset_pid


def upload_files(api: NativeApi, dataset_pid: str, files: Iterable[Path]) -> None:
    for file_path in files:
        print(f"Enviando arquivo: {file_path}")
        datafile = Datafile()
        datafile.set({"pid": dataset_pid, "filename": file_path.name})
        response = api.upload_datafile(dataset_pid, str(file_path), datafile.json())

        if response.status_code not in {200, 201}:
            try:
                detail = response.json()
            except ValueError:
                detail = response.text
            raise RuntimeError(
                f"Falha ao enviar '{file_path}' (status {response.status_code}): {detail}"
            )


def main() -> int:
    args = parse_args()

    if not args.dataverse_alias:
        raise RuntimeError(
            "É necessário informar o alias do Dataverse (--dataverse-alias ou variável DATAVERSE_ALIAS)."
        )

    base_url = ensure_trailing_slash(args.base_url)
    api = NativeApi(base_url, args.api_token)

    dataset = build_dataset(Path(args.dataset_json), args)

    ensure_dataverse_exists(
        api,
        parent_alias=args.parent_dataverse,
        alias=args.dataverse_alias,
        name=args.dataverse_name,
        contact_email=args.dataverse_email,
        affiliation=args.dataverse_affiliation,
        description=args.dataverse_description,
        skip_creation=args.skip_dataverse_creation,
    )
    dataset_pid = create_dataset(api, args.dataverse_alias, dataset)

    files = list(iter_files(Path(args.data_root)))
    if not files:
        print("Nenhum arquivo encontrado para upload, dataset criado sem dados.")
    else:
        upload_files(api, dataset_pid, files)

    print("Publicação concluída com sucesso.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Erro durante a publicação: {exc}", file=sys.stderr)
        sys.exit(1)
