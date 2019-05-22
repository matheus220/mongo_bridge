#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime as dt
from schema import Schema, Regex, And, Or, SchemaError
from mongoengine import *


class item(Document):
    name = StringField(unique=True, required=True)
    className = StringField(required=True)
    params = DictField()
    references = DictField()


class input(Document):
    timestamp = DateTimeField(required=True)
    items = DictField()


class InputMongoDB:
    def __init__(self, db, host="localhost", port=27017):
        connect(db, host=host, port=port)
        self._items = {}
        self._classes = {}
        self._item_added_successfully = []

    def add_item(self, class_name_, name_, **kwargs):
        self._item_added_successfully.append(False)

        if (not isinstance(class_name_, str)) or (not isinstance(name_, str)):
            return False

        class_name = class_name_.lower()
        name = name_.lower()
        data = kwargs.get('data', None)
        references = kwargs.get('references', None)
        params = kwargs.get('params', None)

        item_ = {
            'className': class_name,
            'name': name
        }

        if data:
            data_schema = Schema({And(str, Regex(r'^[^_^$].*')): object})
            try:
                if data_schema.validate(data):
                    item_['data'] = data
                    if 'timestamp' not in data.keys() or 'time' not in data.keys():
                        item_['data']['timestamp'] = dt.datetime.utcnow()
            except:
                return False

        if references:
            references_schema = Schema({And(str, Regex(r'^[^_^$].*')): Or(Regex('^[\+,\-]'), [Regex('^[\+,\-]')])})
            try:
                if references_schema.validate(references):
                    item_['references'] = references
            except:
                return False

        if params:
            params_schema = Schema({And(str, Regex(r'^[^_^$].*')): object})
            try:
                if params_schema.validate(params):
                    item_['params'] = params
            except:
                return False

        self._items[name] = item_
        self._item_added_successfully[-1] = True

        return True

    def send_data(self):
        if not all(self._item_added_successfully):
            del self._item_added_successfully[:]
            self._items.clear()
            raise OperationError

        instances = {}

        for item_name in self._items.keys():
            item_ = self._items[item_name]
            try:
                # Get class of collection
                class_name = item_['className']
                if class_name not in self._classes.keys():
                    cls = type(class_name, (DynamicDocument,),
                               {'name': StringField(required=True), 'name_ref': ReferenceField(item, dbref=True)})
                    self._classes[class_name] = cls
                Class = self._classes[class_name]

                # Save/Update_params of item
                item_instance = item.objects(name=item_['name']).first()
                if item_instance:
                    if 'params' in item_.keys():
                        update_params = dict((("set__params__%s" % k, v) for k, v in item_['params'].iteritems()))
                        item.objects(name=item_['name']).update(**update_params)
                else:
                    if 'params' in item_.keys():
                        item_instance = item(name=item_['name'], className=class_name, params=item_['params']).save()
                    else:
                        item_instance = item(name=item_['name'], className=class_name).save()

                # Save document in 'class_name' collection
                kwargs = {'name': item_['name'], 'name_ref': item_instance}
                if 'data' in item_.keys():
                    kwargs['data'] = item_['data']
                    for key in kwargs['data'].keys():
                        if key[0] == '#' and isinstance(kwargs['data'][key], str):
                            succeeded, server_path = self.save_file(item_['name'], kwargs['data'][key])
                            if succeeded:
                                kwargs['data'][key] = server_path
                            else:
                                raise Exception('Error in save file')
                if 'references' in item_.keys():
                    kwargs['references'] = item_['references']
                if 'params' in item_.keys():
                    kwargs['params'] = item_['params']

                collection_instance = Class(**kwargs)
                collection_instance.save()
                instances[item_name] = collection_instance

                # Treat references
                if 'references' in item_.keys():
                    for cls_refs in item_['references'].keys():
                        if isinstance(item_['references'][cls_refs], str):
                            refs = [item_['references'][cls_refs]]
                        else:
                            refs = item_['references'][cls_refs]

                        for ref in refs:
                            if not item.objects(name=ref[1:]):
                                item(name=ref[1:], className=cls_refs.lower()).save()

                            if ref[0] == '+':
                                item.objects(name=item_['name']).update(
                                    **{("set__references__%s" % ref[1:]):item.objects(name=ref[1:]).first()})
                                item.objects(name=ref[1:]).update(
                                    **{("set__references__%s" % item_['name']): item.objects(name=item_['name']).first()})
                            elif ref[0] == '-':
                                item.objects(name=item_['name']).update(
                                    **{("unset__references__%s" % ref[1:]): item.objects(name=ref[1:]).first()})
                                item.objects(name=ref[1:]).update(
                                    **{("unset__references__%s" % item_['name']): item.objects(name=item_['name']).first()})

            except Exception:
                raise Exception

        try:
            input(timestamp=dt.datetime.utcnow(), items=instances).save()
        except OperationError:
            raise OperationError
        finally:
            del self._item_added_successfully[:]
            self._items.clear()

        return True
    def save_file(self, item_name, local_path):
        SERVER_FOLDER_PATH = '/'
        print(local_path)
        server_path = SERVER_FOLDER_PATH + item_name + '_' + dt.datetime.now().strftime("%Y%m%d%H%M%S%f") + '.' + local_path.split(".")[-1]
        print(server_path)
        return True, server_path

    def add_from_list(self, item_list):

        for item_ in item_list:
            kwargs = {}

            if 'className' not in item_.keys() or 'name' not in item_.keys():
                del self._item_added_successfully[:]
                del self._items[:]
                return False
            else:
                if 'data' in item_.keys():
                    kwargs['data'] = item_['data']
                if 'references' in item_.keys():
                    kwargs['references'] = item_['references']
                if 'params' in item_.keys():
                    kwargs['params'] = item_['params']

            try:
                self.add_item(item_['className'], item_['name'], **kwargs)
            except:
                del self._item_added_successfully[:]
                del self._items[:]
                return False

        return True
