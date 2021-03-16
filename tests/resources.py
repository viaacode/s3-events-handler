#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  resources.py
#
#  Copyleft 2018 VIAA vzw
#  <admin@viaa.be>
#
#  @author: https://github.com/maartends
#
#######################################################################
#
#  ./tests/resources.py
#
#######################################################################

S3_MOCK_ESSENCE_EVENT = """{  
   "Records":[
      {  
         "eventVersion":"0.1",
         "eventSource":"viaa:s3",
         "eventTime":"2019-12-12T00:39:15.049Z",
         "eventName":"ObjectCreated:Put",
         "userIdentity":{  
            "principalId":"Object Owner CN+OR-id"
         },
         "requestParameters":{  
            "sourceIPAddress":"54.93.243.153"
         },
         "responseElements":{  
            "x-viaa-request-id":"bb90f87be822889b21f197b43090cb4a"
         },
         "s3":{
            "domain":{
               "name":"s3"
            },
            "bucket":{
               "name":"MAM_HighresVideo",
               "ownerIdentity":{
                  "principalId":"Bucket Owner CN+OR-id"
               },
               "metadata":{
                  "tenant": "or-rf5kf25",
                  "kind":"ingest"
               }
            },
            "object":{  
               "key":"191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF",
               "size":4248725,
               "eTag":"77930bf06b236e089a22a255e6b28377",
               "metadata":{
                  "md5sum":"1234abcd1234abcd1234abcd1234abcd",
                  "Type": "application/mxf",
                  "Castor-System-Cid":"7da76343ad6bc9f2f739f0595a2756e4",
                  "Content-Md5":"rbyRpD6YijtbdFuFKakLYQ=="
               }
            }
         }
      }
   ]
}"""

S3_MOCK_COLLATERAL_EVENT = """{  
   "Records":[
      {  
         "eventVersion":"0.1",
         "eventSource":"viaa:s3",
         "eventTime":"2019-12-12T00:39:15.049Z",
         "eventName":"ObjectCreated:Put",
         "userIdentity":{  
            "principalId":"Object Owner CN+OR-id"
         },
         "requestParameters":{  
            "sourceIPAddress":"54.93.243.153"
         },
         "responseElements":{  
            "x-viaa-request-id":"bb90f87be822889b21f197b43090cb4b"
         },
         "s3":{
            "domain":{
               "name":"s3"
            },
            "bucket":{
               "name":"mam-collaterals",
               "ownerIdentity":{
                  "principalId":"Bucket Owner CN+OR-id"
               },
               "metadata":{
                  "tenant": "OR-rf5kf25",
                  "kind":"ingest"
               }
            },
            "object":{  
               "key":"TYPE/MEDIAID/blabla.xif",
               "size":4248725,
               "eTag":"77930bf06b236e089a22a255e6b28377",
               "metadata":{
                  "md5sum":"1234abcd1234abcd1234abcd1234abcd",
                  "Type": "application/mxf",
                  "Castor-System-Cid":"7da76343ad6bc9f2f739f0595a2756e4",
                  "Content-Md5":"rbyRpD6YijtbdFuFKakLYQ=="
               }
            }
         }
      }
   ]
}"""

S3_MOCK_REMOVED_EVENT = """{  
   "Records":[
      {  
         "eventVersion":"0.1",
         "eventSource":"viaa:s3",
         "eventTime":"2019-12-12T00:39:15.049Z",
         "eventName":"ObjectRemoved:*",
         "userIdentity":{  
            "principalId":"Object Owner CN+OR-id"
         },
         "requestParameters":{  
            "sourceIPAddress":"54.93.243.153"
         },
         "responseElements":{  
            "x-viaa-request-id":"bb90f87be822889b21f197b43090cb4b"
         },
         "s3":{
            "domain":{
               "name":"s3"
            },
            "bucket":{
               "name":"mam-collaterals",
               "ownerIdentity":{
                  "principalId":"Bucket Owner CN+OR-id"
               },
               "metadata":{
                  "tenant": "OR-rf5kf25",
                  "kind":"ingest"
               }
            },
            "object":{  
               "key":"TYPE/MEDIAID/blabla.xif",
               "size":4248725,
               "eTag":"77930bf06b236e089a22a255e6b28377",
               "metadata":{
                  "md5sum":"1234abcd1234abcd1234abcd1234abcd",
                  "Type": "application/mxf",
                  "Castor-System-Cid":"7da76343ad6bc9f2f739f0595a2756e4",
                  "Content-Md5":"rbyRpD6YijtbdFuFKakLYQ=="
               }
            }
         }
      }
   ]
}"""

S3_MOCK_UNKNOWN_EVENT = """{  
   "Records":[
      {  
         "eventVersion":"0.1",
         "eventSource":"viaa:s3",
         "eventTime":"2019-12-12T00:39:15.049Z",
         "eventName":"ObjectRestore:Post",
         "userIdentity":{  
            "principalId":"Object Owner CN+OR-id"
         },
         "requestParameters":{  
            "sourceIPAddress":"54.93.243.153"
         },
         "responseElements":{  
            "x-viaa-request-id":"bb90f87be822889b21f197b43090cb4b"
         },
         "s3":{
            "domain":{
               "name":"s3"
            },
            "bucket":{
               "name":"mam-collaterals",
               "ownerIdentity":{
                  "principalId":"Bucket Owner CN+OR-id"
               },
               "metadata":{
                  "tenant": "OR-rf5kf25",
                  "kind":"ingest"
               }
            },
            "object":{  
               "key":"TYPE/MEDIAID/blabla.xif",
               "size":4248725,
               "eTag":"77930bf06b236e089a22a255e6b28377",
               "metadata":{
                  "md5sum":"1234abcd1234abcd1234abcd1234abcd",
                  "Type": "application/mxf",
                  "Castor-System-Cid":"7da76343ad6bc9f2f739f0595a2756e4",
                  "Content-Md5":"rbyRpD6YijtbdFuFKakLYQ=="
               }
            }
         }
      }
   ]
}"""

S3_MOCK_ESSENCE_EVENT_WITHOUT_MD5 = """{  
   "Records":[
      {  
         "eventVersion":"0.1",
         "eventSource":"viaa:s3",
         "eventTime":"2019-12-12T00:39:15.049Z",
         "eventName":"ObjectCreated:Put",
         "userIdentity":{  
            "principalId":"Object Owner CN+OR-id"
         },
         "requestParameters":{  
            "sourceIPAddress":"54.93.243.153"
         },
         "responseElements":{  
            "x-viaa-request-id":"bb90f87be822889b21f197b43090cb4a"
         },
         "s3":{
            "domain":{
               "name":"s3"
            },
            "bucket":{
               "name":"MAM_HighresVideo",
               "ownerIdentity":{
                  "principalId":"Bucket Owner CN+OR-id"
               },
               "metadata":{
                  "tenant": "or-rf5kf25",
                  "kind":"ingest"
               }
            },
            "object":{  
               "key":"191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF",
               "size":4248725,
               "eTag":"77930bf06b236e089a22a255e6b28377",
               "metadata":{
                  "Type": "application/mxf",
                  "Castor-System-Cid":"7da76343ad6bc9f2f739f0595a2756e4",
                  "Content-Md5":"rbyRpD6YijtbdFuFKakLYQ=="
               }
            }
         }
      }
   ]
}"""

S3_MOCK_INVALID_EVENT = """{  
   "Records":[
      {  
         "eventVersion":"0.1",
         "eventSource":"viaa:s3",
         "eventTime":"2019-12-12T00:39:15.049Z",
         "eventName":"ObjectCreated:Put",
         "userIdentity":{  
            "principalId":"Object Owner CN+OR-id"
         },
         "requestParameters":{  
            "sourceIPAddress":"54.93.243.153"
         },
         "responseElements":{  
            "x-viaa-request-id":"bb90f87be822889b21f197b43090cb4a"
         },
         "s3":{
            "domain":{
               "name":"s3"
            },
            "bucket":{
               "ownerIdentity":{
                  "principalId":"Bucket Owner CN+OR-id"
               },
               "metadata":{
                  "tenant": "or-rf5kf25",
                  "kind":"ingest"
               }
            },
            "object":{  
               "key":"191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF",
               "size":4248725,
               "eTag":"77930bf06b236e089a22a255e6b28377",
               "metadata":{
                  "md5sum":"1234abcd1234abcd1234abcd1234abcd",
                  "Type": "application/mxf",
                  "Castor-System-Cid":"7da76343ad6bc9f2f739f0595a2756e4",
                  "Content-Md5":"rbyRpD6YijtbdFuFKakLYQ=="
               }
            }
         }
      }
   ]
}"""

METADATA_DICT = {
    "Dynamic": {
        "s3_object_key": "191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF",
        "s3_bucket": "MAM_HighresVideo",
        "PID": "a1b2c3d4e5",
    },
    "Technical": {"Md5": "3d71f15f2d9ed0ef94d7fdc525c95e15"},
}

MOCK_MEDIAHAVEN_EXTERNAL_METADATA = """<?xml version='1.0' encoding='UTF-8'?>
<MediaHAVEN_external_metadata>
  <title>Essence: pid: test_pid</title>
  <description>Main fragment for essence:\n    - filename: 191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF\n    - CP: VRT\n    </description>
  <MDProperties>
    <CP>VRT</CP>
    <CP_id>OR-rf5kf25</CP_id>
    <sp_name>s3</sp_name>
    <PID>test_pid</PID>
    <s3_domain>s3</s3_domain>
    <s3_bucket>MAM_HighresVideo</s3_bucket>
    <s3_object_key>191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF</s3_object_key>
    <dc_source>191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF</dc_source>
    <s3_object_owner>Object Owner CN+OR-id</s3_object_owner>
    <object_level>file</object_level>
    <object_use>archive_master</object_use>
    <ie_type>n/a</ie_type>
    <md5>1234abcd1234abcd1234abcd1234abcd</md5>
  </MDProperties>
</MediaHAVEN_external_metadata>
"""

MOCK_MEDIAHAVEN_EXTERNAL_METADATA_COLLATERAL = """<?xml version='1.0' encoding='UTF-8'?>
<MediaHAVEN_external_metadata>
  <title>Collateral: pid: test_pid</title>
  <description>Subtitles for essence:
    - filename: TYPE/MEDIAID/blabla.xif
    - CP: VRT
    </description>
  <MDProperties>
    <CP>VRT</CP>
    <CP_id>OR-rf5kf25</CP_id>
    <sp_name>s3</sp_name>
    <PID>test_pid</PID>
    <s3_domain>s3</s3_domain>
    <s3_bucket>mam-collaterals</s3_bucket>
    <s3_object_key>TYPE/MEDIAID/blabla.xif</s3_object_key>
    <dc_source>TYPE/MEDIAID/blabla.xif</dc_source>
    <s3_object_owner>Object Owner CN+OR-id</s3_object_owner>
    <dc_identifier_localid>media_id</dc_identifier_localid>
    <object_level>file</object_level>
    <object_use>metadata</object_use>
    <ie_type>n/a</ie_type>
    <md5>1234abcd1234abcd1234abcd1234abcd</md5>
    <dc_relations>
      <is_verwant_aan>test_pid</is_verwant_aan>
    </dc_relations>
  </MDProperties>
</MediaHAVEN_external_metadata>
"""
# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
