import csv
import json
import logging
from datetime import datetime, timedelta
from supabase import create_client
import schedule
import time
import chardet
import os
from collections import defaultdict

def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(log_dir, "service.log")),
            logging.StreamHandler()
        ]
    )

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
        return result['encoding']

def load_csv_data(csv_path, required_columns):
    encoding = detect_encoding(csv_path)
    logging.info(f"Codificação detectada: {encoding}")

    with open(csv_path, mode='r', encoding=encoding) as file:
        reader = csv.DictReader(file, delimiter=';')

        if not reader.fieldnames:
            raise ValueError("Nenhuma coluna detectada no CSV.")

        missing_columns = [col for col in required_columns if col not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias ausentes no CSV: {missing_columns}")

        return list(reader)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r') as file:
        return json.load(file)

def connect_to_supabase(config):
    return create_client(config["supabase_url"], config["supabase_service_role_key"])

def organize_csv_by_table(csv_data):
    organized_data = defaultdict(list)
    
    for row in csv_data:
        if "Cód" in row:
            organized_data['cliente'].append(row)
        if "Núm.contrato" in row:
            organized_data['contrato'].append(row)
        if "Código" in row:
            organized_data['produto'].append(row)

    return organized_data

def fetch_existing_data(supabase):
    logging.info("Carregando dados existentes do Supabase.")

    cliente_data = supabase.table('cliente').select('*').execute().data
    contrato_data = supabase.table('contrato').select('*').execute().data
    produto_data = supabase.table('produto').select('*').execute().data

    return {
        'cliente': {item['id_siger_cliente']: item for item in cliente_data},
        'contrato': {item['id_contrato']: item for item in contrato_data},
        'produto': {item['id_produto_siger']: item for item in produto_data}
    }

def compare_and_prepare_batches(table_name, csv_data, existing_data, transform_func):
    new_records = []
    updated_records = []
    skipped_records = 0

    for row in csv_data:
        record = transform_func(row)
        unique_key = list(existing_data.keys())[0] if existing_data else None

        if unique_key and record[unique_key] in existing_data:
            existing_record = existing_data[record[unique_key]]
            if existing_record != record:
                updated_records.append(record)
            else:
                skipped_records += 1
        else:
            new_records.append(record)

    logging.info(f"Tabela '{table_name}': {len(new_records)} novos, {len(updated_records)} atualizados, {skipped_records} ignorados.")
    return new_records, updated_records

def sync_batches(supabase, table_name, new_records, updated_records):
    if new_records:
        supabase.table(table_name).insert(new_records).execute()
        logging.info(f"Inseridos {len(new_records)} novos registros em '{table_name}'.")
    if updated_records:
        for record in updated_records:
            unique_key = list(record.keys())[0]
            supabase.table(table_name).update(record).eq(unique_key, record[unique_key]).execute()
        logging.info(f"Atualizados {len(updated_records)} registros em '{table_name}'.")

def transform_cliente(row):
    return {
        "id_siger_cliente": int(row["Cód"].strip()),
        "nome_cliente": row.get("Razão social", "").strip(),
        "cnpj": row.get("CNPJ/CPF", "").strip(),
        "deletado": row.get("Descr.Sit.item cont.(enumerado)", "").strip().lower() != "ativo"
    }

def transform_contrato(row):
    return {
        "id_contrato": int(row["Núm.contrato"].strip()),
        "id_siger_cliente": int(row["Cód"].strip()),
        "dt_inic_cont": row.get("Dt.inc.cont", "").strip(),
        "dt_vig_inic": row.get("Dt.vig.inic", "").strip(),
        "dt_vig_final": row.get("Dt.vig.final", "2099-01-01").strip(),
        "deletado": row.get("Descr.Sit.item cont.(enumerado)", "").strip().lower() != "ativo"
    }

def transform_produto(row):
    return {
        "id_produto_siger": int(row["Código"].strip()),
        "nome_produto": row["Desc.item"].strip(),
        "tipo_produto": row["Descrição"].strip(),
        "num_serie": row.get("Núm.lote forn", "").strip(),
        "num_lote": row.get("Núm.lote", "").strip(),
        "ativo": row.get("Descr.Sit.item cont.(enumerado)", "").strip().lower() == "ativo"
    }

def sync_data():
    logging.info("Iniciando sincronização dos dados.")
    start_time = datetime.now()

    try:
        config = load_config()
        supabase = connect_to_supabase(config)

        required_columns = [
            "Cód", "Razão social", "CNPJ/CPF",
            "Núm.contrato", "Dt.inc.cont", "Dt.vig.inic", "Dt.vig.final",
            "Código", "Desc.item", "Descrição"
        ]

        csv_data = load_csv_data(config["csv_file_path"], required_columns)
        organized_data = organize_csv_by_table(csv_data)
        existing_data = fetch_existing_data(supabase)

        for table_name, csv_rows in organized_data.items():
            new_records, updated_records = compare_and_prepare_batches(
                table_name, csv_rows, existing_data.get(table_name, {}), globals()[f"transform_{table_name}"]
            )
            sync_batches(supabase, table_name, new_records, updated_records)

        end_time = datetime.now()
        elapsed_time = str(timedelta(seconds=(end_time - start_time).total_seconds()))
        logging.info(f"Sincronização concluída em {elapsed_time}.")
    except Exception as e:
        logging.error(f"Erro durante a sincronização: {e}")

def start_service():
    config = load_config()
    interval = config["update_interval_minutes"]
    logging.info(f"Serviço iniciado. Atualização programada a cada {interval} minutos.")

    schedule.every(interval).minutes.do(sync_data)

    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logging.error(f"Erro no loop de agendamento: {e}")
        time.sleep(1)

if __name__ == "_main_":
    setup_logging()
    start_service()