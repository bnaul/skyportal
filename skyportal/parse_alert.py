import avro.io
import avro.schema
import fastavro
import io
import json
import sys
import glob
import os.path
import hashlib


def combine_schemas(schema_files):
    """Combine multiple nested schemas into a single schema.
    """
    known_schemas = avro.schema.Names()

    for s in schema_files:
        schema = load_single_avsc(s, known_schemas)
    return schema.to_json()


def load_single_avsc(file_path, names):
    """Load a single avsc file.
    """
    with open(file_path) as file_text:
        json_data = json.load(file_text)
    schema = avro.schema.SchemaFromJSONData(json_data, names)
    return schema


def load_stamp(file_path):
    """Load a cutout postage stamp file to include in alert.
    """
    _, fileoutname = os.path.split(file_path)
    with open(file_path, mode='rb') as f:
        cutout_data = f.read()
        cutout_dict = {"fileName": fileoutname, "stampData": cutout_data}
    return cutout_dict


def read_avro_data(bytes_io, json_schema):
    """Read avro data with fastavro module and decode with a given schema.
    """
#    bytes_io.seek(
#        0)  # force schemaless_reader to read from the start of stream, byte offset = 0
    message = fastavro.schemaless_reader(bytes_io, json_schema)
    return message


def read_avro_data(bytes_reader, avro_schema):
    """Read avro data and decode with a given schema.
    """
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(avro_schema)
    message = reader.read(decoder)
    return message


if __name__ == "__main__":
    DATA_DIR = os.path.join(os.path.dirname(__file__), 'tests', 'data')
    json_path = os.path.join(DATA_DIR, 'alert.json')
    schema_files = [f'{DATA_DIR}/{v}.avsc'
                    for v in ['candidate', 'prv_candidate', 'cutout', 'alert']]
    cutoutsci_path = os.path.join(DATA_DIR,
                                  'candid-87704463155000_pid-8770446315_targ_sci.jpg')
    cutouttemp_path = os.path.join(DATA_DIR, 'candid-87704463155000_ref.jpg')
    cutoutdiff_path = os.path.join(DATA_DIR,
                                   'candid-87704463155000_pid-8770446315_targ_scimref.jpg')
    avro_path = os.path.join(DATA_DIR, '87704463155000.avro')

    alert_schema = avro.schema.Parse(json.dumps(combine_schemas(schema_files)))


    with open(json_path) as file_text:
        json_data = json.load(file_text)

    # Load science stamp if included
    if cutoutsci_path is not None:
        cutoutTemplate = load_stamp(cutoutsci_path)
        json_data['cutoutScience'] = cutoutTemplate

    # Load template stamp if included
    if cutouttemp_path is not None:
        cutoutTemplate = load_stamp(cutouttemp_path)
        json_data['cutoutTemplate'] = cutoutTemplate

    # Load difference stamp if included
    if cutoutdiff_path is not None:
        cutoutDifference = load_stamp(cutoutdiff_path)
        json_data['cutoutDifference'] = cutoutDifference


    avro_bytes = open(avro_path, 'rb')
    message = read_avro_data(avro_bytes, alert_schema)
