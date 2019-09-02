from google.cloud import datastore
import config
import os


class DBProcessor(object):
    def __init__(self):
        self.client = datastore.Client()
        self.meta = config.AFAS_DB_PROCESSOR[os.environ.get('DATA_SELECTOR', 'Required parameter is missed')]
        pass

    def process(self, payload):
        kind, key = self.identity(payload)
        entity_key = self.client.key(kind, key)
        entity = self.client.get(entity_key)

        if entity is None:
            entity = datastore.Entity(key=entity_key)

        self.populate_from_payload(entity, payload)
        self.client.put(entity)

    def identity(self, payload):
        return self.meta['entity_name'], self.meta['id_property']

    @staticmethod
    def populate_from_payload(entity, payload):
        for name in payload.keys():
            value = payload[name]
            entity[name] = value

#
# class EmployeeProcessor(DBProcessor):
#
#     def __init__(self):
#         DBProcessor.__init__(self)
#
#     @staticmethod
#     def selector():
#         return 'employee'
#
#     def identity(self, payload):
#         return config.EMPLOYEE_ENTITY_NAME, payload['email_address']
#
#
# class DepartmentProcessor(DBProcessor):
#
#     def __init__(self):
#         DBProcessor.__init__(self)
#
#     @staticmethod
#     def selector():
#         return 'department'
#
#     def identity(self, payload):
#         return config.DEPARTMENT_ENTITY_NAME, payload['Afdeling']
#
#
# class BusinessUnitProcessor(DBProcessor):
#
#     def __init__(self):
#         DBProcessor.__init__(self)
#
#     @staticmethod
#     def selector():
#         return 'businessunit'
#
#     def identity(self, payload):
#         return config.BU_ENTITY_NAME, payload['afas_id']
#
#
# class CompanyProcessor(DBProcessor):
#
#     def __init__(self):
#         DBProcessor.__init__(self)
#
#     @staticmethod
#     def selector():
#         return 'company'
#
#     def identity(self, payload):
#         return config.COMPANY_ENTITY_NAME, payload['afas_id']
#
