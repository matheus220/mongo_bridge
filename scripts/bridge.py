#!/usr/bin/env python
# coding: utf-8

import os
import sys
import yaml
from importlib import import_module

import rospy
from data_capture.srv import *
from mongo_bridge.msg import DataArray
from mongoengine import Document, FieldDoesNotExist, ValidationError, connect


class MongoBridge(object):
    TAG = "MongoBridge"

    def __init__(self):
        rospy.init_node("mongo_bridge")
        connect("robotics", host="localhost", port=27017)
        classPaths = os.environ.get("MONGO_CLASS_PATH", "")
        for path in classPaths.split(":"):
            sys.path.append(path)
        self.classes = dict()
        # Data subscriber
        rospy.Subscriber("/mongo_bridge/data", DataArray, self.dataCallback)
        rospy.Service('/mongo_bridge/save', RequestSaveData, self.handleData)
        rospy.loginfo("{} initialized".format(self.TAG))

    def dataCallback(self, message):
        for data in message.data:
            self.onData(data)

    def handleData(self, req):
        response = RequestSaveDataResponse()

        for data in req.data:
            if self.onData(data):
                response.status.append('succeeded')
            else:
                response.status.append('failed')

        return response

    def onData(self, data):
        """
        :type message: DataArray
        """
        jsonData = yaml.safe_load(data.jsonData)
        try:
            key = "{}.{}".format(data.dataModule, data.dataClass)
            if key in self.classes.keys():
                Class = self.classes[key]
            else:
                Class = getattr(import_module(data.dataModule), data.dataClass)
                if not issubclass(Class, Document):
                    rospy.logwarn(
                        "{}::onData Ignoring data class '{}' because it is not a subclass of Document".format(
                            self.TAG, data.dataClass))
                    return False
                self.classes[key] = Class
            if isinstance(jsonData, list):
                for datum in jsonData:
                    instance = Class(**datum)
                    instance.save()
            else:
                instance = Class(**jsonData)
                instance.save()
            rospy.loginfo("{}::onData Saved instance of {}".format(self.TAG, data.dataClass))
            return True
        except ImportError as e:
            rospy.logwarn("{}::onData ImportError with data class '{}': {}".format(self.TAG, data.dataClass, e))
        except AttributeError as e:
            rospy.logwarn(
                "{}::onData AttributeError with data class '{}': {}".format(self.TAG, data.dataClass, e))
        except FieldDoesNotExist as e:
            rospy.logwarn("{}::onData FieldDoesNotExist ...........: {}".format(self.TAG, e))
        except ValidationError as e:
            rospy.logwarn("{}::onData ValidationError .............: {}".format(self.TAG, e))

        return False

if __name__ == "__main__":
    mongoBridge = MongoBridge()
    rospy.spin()
