from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type, Optional
import json
import csv
import db_api
from pydoc import locate
from json.decoder import JSONDecodeError

from dataclasses_json import dataclass_json

DB_ROOT = Path('db_files')
RECORDS_PER_FILE = 20


class DataBaseError(Exception):
    pass


global Tables


@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type

#########################################################


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any

#########################################################
class HashTable:
    table: Dict

    def __init__(self, dict: Dict):
        self.table = dict

    def insert(self, key: Any, PK: Any):
        self.table[key].append(PK)

    def raw(self):
        return self.table

    def contains(self, item):
        return item in self.table

    def get(self, item: Any):
        try:
            return self.table[item]
        except:
            raise ValueError

    def delete(self, key: Any, PK: Any):
        self.get(key).remove(PK)

#-------------------------------------------------------#

class PKHashTable(HashTable):
    def insert(self, key: Any, value: Dict[str, Any]):
        self.table[key] = value

    def delete(self, key: Any, PK: Any = 0):
        cast_to = type(list(self.table.keys())[0])
        del self.table[int(key)]

#########################################################

@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    # name: str
    # fields: List[DBField]
    # key_field_name: str
    files: List [str]
    m_count: int
    indexing: Dict [str, Any]

    def __init__(self, name: str, fields: List [DBField], key_field_name: str, files: List [str] = None,
                 count: int = None):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.files = files
        self.m_count = count
        if files is None:
            self.files = [f'{name}1.csv']
            with (DB_ROOT / Path(name + '1.csv')).open('w+'):
                pass
            self.m_count = 0
            self.indexing = self.create_PK_index()
            self.create_meta_data()
        else:
            self.indexing = self.load_indexes()

    def create_meta_data(self):
        with (DB_ROOT / f"{self.name}_meta_data.json").open('w+') as f:
            pass
        self.update_meta_data()

    def create_PK_index(self):
        with (DB_ROOT / f"{self.name}_{self.key_field_name}.json").open('w+'):
            pass
        dic = dict(PK_index=PKHashTable({}), additional={})
        return dic

    def update_meta_data(self):
        with (DB_ROOT / f"{self.name}_meta_data.json").open('w+') as file:
            try:
                meta_data = json.load(file)
            except JSONDecodeError:
                meta_data = {}
            meta_data['indexes'] = ["PK_index"] + list(self.indexing["additional"].keys())
            json.dump(meta_data, file)

    def load_indexes(self):
        dic = {"additional":{}}
        with (DB_ROOT / f"{self.name}_meta_data.json").open('r+') as file:
            meta_data = json.load(file)
            dic["PK_index"] = self.load_index(self.key_field_name)
            for field in meta_data['indexes'][1:]:
                dic["additional"][field] = self.load_index(field)
        return dic

    def load_index(self, field):
        if field == self.key_field_name:
            with (DB_ROOT / f"{self.name}_{field}.json").open('r+') as file:
                return PKHashTable(json.load(file))
        with (DB_ROOT / f"{self.name}_{field}.json").open('r+') as file:
            return HashTable(json.load(file))

    def count(self) -> int:
        return self.m_count

    def validate_PK(self, values: Dict[str, Any]) -> None:
        key = values[self.key_field_name]
        if self.indexing['PK_index'].contains(key):
            raise ValueError("Key Duplicate")

    def valid_values(self, values: Dict[str, Any]) -> None:
        for field in values:
            if type(values[field]) is not self.fields[self.get_index(field)].type:
                raise DataBaseError("Field types don't match")
        if self.key_field_name in values:
            self.validate_PK(values)

    def dict_to_csv(self, values: Dict[str, Any]) -> List [str]:
        return [values [key.name] for key in self.fields]

    def insert_record(self, values: Dict[str, Any]) -> None:
        self.valid_values(values)
        self.create_new_file_if_necessary()
        file_4_insertion = self.files [-1]
        self.m_count += 1
        with (DB_ROOT / file_4_insertion).open('a', newline='') as wf, (DB_ROOT / file_4_insertion).open('r') as rf:
            reader = csv.reader(rf)
            row_num = len(list(reader))
            w = csv.writer(wf)
            w.writerow(self.dict_to_csv(values))
        self.update_indexes(file_4_insertion, row_num, values)
        self.back_up_all_indexes()
        self.update_meta_data()
        update_meta_data()

    def update_indexes(self, file_4_insertion: str, row_num: int, values: Dict[str, Any]):
        for field, index in self.indexing['additional'].items():
            index.insert(values[field], values[self.key_field_name])
        self.indexing["PK_index"].insert(values[self.key_field_name],(file_4_insertion, row_num))

    def delete_indexes(self, values: Dict[str, Any]):
        for field, index in self.indexing['additional'].items():
            index.delete(values[field], values[self.key_field_name])
        self.indexing["PK_index"].delete(values[self.key_field_name])

    def back_up_all_indexes(self):
        with (DB_ROOT / f"{self.name}_{self.key_field_name}.json").open('w') as f:
            json.dump(self.indexing["PK_index"].raw(), f)
        for field, index in self.indexing["additional"].items():
            with (DB_ROOT / f"{self.name}_{field}.json").open('w') as f:
                json.dump(index.raw(), f)

    def create_new_file_if_necessary(self) -> None:
        if self.m_count % RECORDS_PER_FILE == 0 and self.m_count != 0:
            num = len(self.files) + 1
            with (DB_ROOT / f"{self.name}{num}.csv").open('w+'):
                pass
            self.files.append(f"{self.name}{num}.csv")
            update_meta_data()

    def row_2_dict(self, row: List):
        return {field.name: row[i] for i, field in enumerate(self.fields)}

    def delete_record(self, key: Any) -> None:
        file_name, row_num = self.indexing["PK_index"].get(key)
        self.m_count -= 1
        update_meta_data()
        with(DB_ROOT / file_name).open('r') as f:
            block = [line[:-1].split(',') for line in list(f)]
        old_row = block[row_num]
        new_row, PK = self.get_last_line()
        self.replace_row(old_row, block, file_name, row_num, new_row)
        self.write_block(block, file_name)
        self.back_up_all_indexes()

    def write_block(self,block, file_name):
        with(DB_ROOT / file_name).open('w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(block)

    def replace_row(self, old_row, block, file_name, row_num, new_row):
        dic_old_row = self.row_2_dict(old_row)
        self.delete_indexes(dic_old_row)
        if old_row == new_row:
            return
        block[row_num] = new_row
        new_row = self.row_2_dict(new_row)
        self.update_indexes(file_name, row_num, new_row)

    def get_last_line(self):
        file = self.files[-1]
        with (DB_ROOT / file).open('r') as f:
            block = [line[:-1].split(',') for line in list(f)]
        if block[:-1]:
            with(DB_ROOT / file).open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(block[:-1])
        else:
            Path.unlink(Path(DB_ROOT / file))
            self.files.remove(file)
            update_meta_data()
        return block[-1], block[-1][self.get_index_of_PK()]

    def line_meets_criterias(self, line, indices, criteria) -> bool:
        operators = {'<': lambda x, y: x < y,
                     '>': lambda x, y: x > y,
                     '=': lambda x, y: x == y,
                     '<=': lambda x, y: x <= y,
                     '>=': lambda x, y: x >= y}
        for i, c in enumerate(criteria):
            cast_to = type(c.value)
            if not operators[c.operator]((cast_to)(line[indices[i]]), c.value):
                return False
        return True

    def delete_records(self, criteria: List [SelectionCriteria]) -> None:
        for file in self.files:
            num_del = 0
            with(DB_ROOT / file).open('r') as f:
                block = [line[:-1].split(',') for line in list(f)]
            row_num = 0
            while row_num < len(block):
                num_del += self.delete_row(block, file, row_num, criteria)
                row_num += 1
            if num_del != 0:
                self.write_block(block, file)
                self.m_count -= num_del
        self.back_up_all_indexes()

    def delete_row(self, block, file,  row_num, criteria):
        indices = [[field.name for field in self.fields].index(c.field_name) for c in criteria]
        num_del = 0
        while self.line_meets_criterias(block[row_num], indices, criteria):
            num_del += 1
            new_row, PK = self.get_last_line()
            if new_row == block[row_num]:
                return num_del
            self.replace_row(block[row_num], block, file, row_num, new_row)
        return num_del

    def get_index_of_PK(self):
        return self.get_index(self.key_field_name)

    def get_record(self, key: Any) -> Optional[dict]:
        file_name, row_num = self.indexing["PK_index"].get(key)
        with(DB_ROOT / file_name).open('r') as f:
            block = [line[:-1].split(',') for line in list(f)]
        return self.row_2_dict(block[row_num])

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        file_name, row_num = self.indexing["PK_index"].get(key)
        self.valid_values(values)
        with(DB_ROOT / file_name).open('r') as f:
            block = [line[:-1].split(',') for line in list(f)]
        block[row_num] = self.update_row(block[row_num], values)
        with(DB_ROOT / file_name).open('w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(block)


    def update_row(self, row, values):
        for field in values:
            row[self.get_index(field)] = values[field]
        return row

    def get_index(self, field):
        return [field.name for field in self.fields].index(field)

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        indices = [[field.name for field in self.fields].index(c.field_name) for c in criteria]
        result = []
        for file in self.files:
            with(DB_ROOT / file).open('r') as f:
                lis = [line[:-1].split(',') for line in list(f)]
                res = list(filter(lambda line: self.line_meets_criterias(line, indices, criteria), lis))
                result += list(map(lambda line: {field.name: line [i] for i, field in enumerate(self.fields)}, res))
        return result

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError

    def delete(self):
        for file in self.files:
            Path.unlink(Path(DB_ROOT / file))

    def dict(self):
        return {"name": self.name,
                "fields": {field.name: field.type.__name__ for field in self.fields},
                "key_field_name": self.key_field_name,
                "files": self.files,
                "m_count": self.m_count}

#########################################################

@dataclass_json
@dataclass
class DataBase(db_api.DataBase):

    def __init__(self):
        global Tables
        if (DB_ROOT / "DataBase.json").exists():
            with (DB_ROOT / "DataBase.json").open('r') as meta_data_file:
                meta_data = json.load(meta_data_file)
            Tables = self.decode_tables(meta_data)
        else:
            with (DB_ROOT / "DataBase.json").open('w+'):
                pass
            Tables = {}

    def create_table(self,
                     table_name: str,
                     fields: List [DBField],
                     key_field_name: str) -> DBTable:
        if self.table_exists(table_name):
            raise DataBaseError("Table Already Exists")
        self.validate_PK(key_field_name, fields)

        global Tables
        table = DBTable(table_name, fields, key_field_name)
        Tables[table_name] = table
        update_meta_data()
        return table

    def table_exists(self, table_name):
        global Tables
        return table_name in Tables

    def validate_PK(self, key_field_name, fields):
        if key_field_name not in [field.name for field in fields]:
            raise ValueError

    def num_tables(self) -> int:
        global Tables
        return len(Tables.keys())

    def get_table(self, table_name: str) -> DBTable:
        global Tables
        return Tables [table_name]

    def delete_table(self, table_name: str) -> None:
        global Tables
        Tables [table_name].delete()
        del (Tables [table_name])
        update_meta_data()

    def get_tables_names(self) -> List [Any]:
        global Tables
        return list(Tables.keys())

    def query_multiple_tables(
            self,
            tables: List [str],
            fields_and_values_list: List [List [SelectionCriteria]],
            fields_to_join_by: List [str]
    ) -> List [Dict [str, Any]]:
        raise NotImplementedError

    def decode_tables(self, meta_data: List [Dict]):
        dic = {}
        for table in meta_data:
            dic [table ['name']] = DBTable(table ['name'], self.dict_to_fields(table ['fields']),
                                           table ['key_field_name'], table ['files'], table ['m_count'])
        return dic

    def dict_to_fields(self, dict: Dict):
        return [DBField(field, locate(type)) for field, type in dict.items()]


# --------------GLOBAL FUNCTIONS-------------------
def update_meta_data():
    dict_for_json = [table.dict() for table in Tables.values()]
    with open(DB_ROOT / "DataBase.json", 'w') as file:
        json.dump(dict_for_json, file)
