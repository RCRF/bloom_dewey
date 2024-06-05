import pytest
import boto3
import requests_mock
from moto import mock_aws
from pathlib import Path
from sqlalchemy.orm import sessionmaker

from bloom_lims.bdb import BLOOMdb3
from bloom_lims.bdb import BloomWorkflow, BloomFile
import sys


@pytest.fixture
def s3_bucket():
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'daylily-dewey-0'
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name

@pytest.fixture
def db_session():
    bdb=BLOOMdb3()    
    yield bdb

@pytest.fixture
def bloom_file_instance(db_session, s3_bucket):
    return BloomFile(db_session, bucket_prefix="daylily-dewey-")

def test_create_file_no_data(bloom_file_instance):
    new_file = bloom_file_instance.create_file(metadata={"description": "No data test"})
    assert new_file is not None
    assert new_file.json_addl['properties']['description'] == "No data test"
    assert 's3_key' not in new_file.json_addl['properties']

def test_create_file_with_data(bloom_file_instance):
    data_path = Path("tests/test_pdf.pdf")
    with open(data_path, "rb") as f:
        data = f.read()
    new_file = bloom_file_instance.create_file(metadata={"description": "Data test"}, data=data, data_file_name=data_path.name)
    assert new_file is not None
    assert new_file.json_addl['properties']['description'] == "Data test"
    assert 's3_key' in new_file.json_addl['properties']
    assert new_file.json_addl['properties']['file_size'] == len(data)

def test_create_file_with_local_path(bloom_file_instance):
    data_path = Path("tests/test_pdf.pdf")
    new_file = bloom_file_instance.create_file(metadata={"description": "Local path test"}, full_path_to_file=str(data_path))
    assert new_file is not None
    assert new_file.json_addl['properties']['description'] == "Local path test"
    assert 's3_key' in new_file.json_addl['properties']
    assert new_file.json_addl['properties']['file_size'] == data_path.stat().st_size

def test_create_file_with_url(bloom_file_instance):
    url = "https://github.com/Daylily-Informatics/bloom/blob/20240603b/tests/test_png.png"
    with requests_mock.Mocker() as m:
        m.get(url, content=b"test content")
        new_file = bloom_file_instance.create_file(metadata={"description": "URL test"}, url=url)
        assert new_file is not None
        assert new_file.json_addl['properties']['description'] == "URL test"
        assert 's3_key' in new_file.json_addl['properties']
        assert new_file.json_addl['properties']['file_size'] == len(b"test content")

if __name__ == "__main__":
    pytest.main()
