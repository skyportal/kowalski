import os

from astropy.io import fits
from astropy.wcs import WCS

import requests
import json
import psycopg2
from datetime import datetime

import pickle
import gzip
import io
import numpy as np

import avro
import avro.io
import avro.schema
from avro.schema import SchemaFromJSONData
import avro.tool
import confluent_kafka
import fastavro

import glob

def makebitims(image):
        ######################################################
        # make bit images of the cutouts for the marshal
        #
        # Inputs:
        # image: input image cutout
        #
        # Returns:
        # buf2: a gzipped fits file of the cutout image as
        #  a BytesIO object
        ######################################################

        # open buffer and store image in memory
        buf = io.BytesIO()
        buf2 = io.BytesIO()
        fits.writeto(buf, image)
        with gzip.open(buf2, "wb") as fz:
            fz.write(buf.getvalue())

        return buf2

def write_avro_data(json_dict, avro_schema):
        """Encode json into avro format given a schema.
        For testing packet integrity.
        Args:
            json_dict (dict): data to put into schema, can be dict.
            avro_schema (avro.schema.RecordSchema): output schema.
        Returns:
            (io.BytesIO object): input json in schema format.
        """
        writer = avro.io.DatumWriter(avro_schema)
        bytes_io = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_io)
        writer.write(json_dict, encoder)
        return bytes_io

def create_alert_packet(cand, scicut, refcut, diffcut):
        """Create top level avro packet from input candidate.
        Args:
            cand (dict): all data of a single candidate.
            scicut (bytes): science image cutout.
            refcut (bytes): reference image cutout.
            diffcut (bytes): difference image cutout.
        Returns:
            (dict): schema in dictionary format.
        """
        prev_cands = cand["prv_candidates"]
        alert = {
            "schemavsn": "0.1",
            "publisher": "TURBO_test",
            "cutoutScience": scicut,
            "cutoutTemplate": refcut,
            "cutoutDifference": diffcut,
            "objectId": cand["objectId"],
            "candid": cand["candid"],
            "candidate": cand,
            "prv_candidates": prev_cands,
        }
        return alert

def combine_schemas(schema_files):
        """Combine multiple nested schemas into a single schema.
        Modified from https://github.com/dekishalay/pgirdps
        Args:
            schema_files (list[str]): list of file paths to .avsc schemas.
        Returns:
            (dict): built avro schema.
        """
        known_schemas = avro.schema.Names()  # avro.schema.Names object
#        print(known_schemas)
#        exit()
        
        schema_files=["candidate.avsc", "prv_candidate.avsc", "alert.avsc"]

        for s in schema_files:
#            print(s)
            schema = load_single_avsc(s, known_schemas)
#        exit()
        # using schema.to_json() doesn't fully propagate the nested schemas
        # work around as below
        props = dict(schema.props)
        fields_json = [field.to_json() for field in props["fields"]]
        props["fields"] = fields_json

        return props

def load_single_avsc(file_path, names):
        """Load a single avsc file.
        Modified from https://github.com/dekishalay/pgirdps
        Args:
            file_path (str): file path of the .avsc schema to build.
            names (avro.schema.Names): an avro schema.
        Returns:
            (avro.schema.RecordSchema): data in avro schema format.
        """
        curdir = os.path.dirname(__file__)
        file_path = os.path.join(curdir, file_path)

        with open(file_path) as file_text:
            json_data = json.load(file_text)

        # SchemaFromJSONData not working
        # ##### works only with avro version 1.10.1 #####
        schema = SchemaFromJSONData(json_data, names)

        return schema

#def save_local(candid, records, schema):
#        """Save avro file in given schema to output subdirectory.
#        Args:
#            candid (number type): candidate id.
#            records (list): a list of dictionaries
#            schema (avro.schema.RecordSchema): schema definition.
#        """
##        self._make_avro_output_dir()
#        avro_packet_path = os.path.join(
#            self.get_sub_output_dir(), str(candid) + ".avro"
#        )
#        out = open(avro_packet_path, "wb")
#        # logger.info(f'out file: {out}')
#        fastavro.writer(out, schema, records)
#        out.close()

def grab_psql_TURBO():
        DB_NAME = "turbo"
        DB_USER = "mtran"
        DB_PASS = "TURBOTURBO"
        DB_HOST = "127.0.0.1"
        DB_PORT = "5432"
        conn = psycopg2.connect(
            database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)

        cur = conn.cursor()

        qSelect = "SELECT * FROM rs_test_pipeline7.candidates"

        cur.execute(qSelect)

        results=cur.fetchall()
       # print(results)
       # exit()
#        restuls2=[results[6],results[7],results[8],results[-1],results[0]]
#        print(results[28])
        for result in results:
            alert={"schemavsn": "0.1","publisher": "turbo_test"} #dictionary for kowalski
            result2=[str(result[6]),str(result[7]),str(result[8]),str(result[-1]),str(result[0])]

#            keys=['candid','ra','dec','ra_cand','dec_cand','real_bogus','cutoutScience','cutoutTemplate','cutoutDifference','time','objectId']
            keys=['cutoutScience','cutoutTemplate','cutoutDifference','objectId','candid']
            keys2=['jd', 'diffmaglim', 'fid', 'programpi', 'programid', 'candid', 'isdiffpos', 'field', 'xpos', 'ypos', 'ra', 'dec', 'magpsf', 'sigmapsf', 'chipsf', 'magap', 'sigmagap', 'distnr', 'magnr', 'sigmagnr', 'chinr', 'sharpnr', 'sky', 'magdiff', 'fwhm', 'classtar', 'mindtoedge', 'magfromlim', 'seeratio', 'aimage', 'bimage', 'aimagerat', 'bimagerat', 'elong', 'nneg', 'nbad', 'rb', 'rbversion', 'ssdistnr', 'ssmagnr', 'ssnamenr', 'sumrat', 'magapbig', 'sigmagapbig', 'psra1', 'psdec1', 'jdstarthist', 'jdendhist', 'scorr', 'tooflag', 'psobjectid1', 'sgmag1', 'srmag1', 'simag1', 'szmag1', 'tmjmag1', 'tmhmag1', 'tmkmag1', 'tmobjectid1', 'sgscore1', 'distpsnr1', 'psobjectid2', 'sgmag2', 'srmag2', 'simag2', 'szmag2', 'sgscore2', 'distpsnr2', 'tmjmag2', 'tmhmag2', 'tmkmag2', 'tmobjectid2', 'psobjectid3', 'sgmag3', 'srmag3', 'simag3', 'szmag3', 'sgscore3', 'distpsnr3', 'tmjmag3', 'tmhmag3', 'tmkmag3', 'tmobjectid3', 'nmtchps', 'nmtchtm', 'dsnrms', 'ssnrms', 'dsdiff', 'magzpsci', 'magzpsciunc', 'magzpscirms', 'clrcoeff', 'clrcounc', 'zpclrcov', 'zpmed', 'clrmed', 'clrrms', 'neargaia', 'neargaiabright', 'maggaia', 'maggaiabright', 'exptime']
#            if len(keys)==len(result2):
#                #result=results[28]
            for i in range(len(result2)):
                if 'cutout' in keys[i]:
                    j=i+6
#                hdul=fits.open(f"{result[tmp]}.gz") #remove the .gz form the final version as the files will be saved in that format moving forward
#                    data=hdul[0].data
                    data=fits.getdata(f"{result[j]}.gz")
                    tmp=makebitims(data.astype(np.float32))
#                    tmp.seek(0)
#                    data_bytes=tmp.read()
                    data_bytes=tmp.getvalue()
#                    buf = io.BytesIO()
#                    buf2 = io.BytesIO()
#                    fits.writeto(buf,data)
#                    with gzip.open(buf2, "wb") as fz:
#                        fz.write(buf.getvalue())
#                        print(buf.getvalue())
#                    data_bytes=data.tobytes()
#                    with gzip.open(buf2,"rb") as f:
#                        data_bytes=f.read()
#                    c=open(f"{result[i]}.gz",'rb').read()
#                    data_bytes=bytearray(c)
#                    data_bytes=buf2
#                    print(data_bytes)
                    alert[keys[i]]=data_bytes
                else:
                    alert[keys[i]]=result[i]
#            else:
#            print(len(keys),len(
#                print('Length of keys does not match length of items. Dictionary cant be created.')

            candic={}
            for i in keys2:
                if i=='ra':
                    candic['ra']=result[3]
                elif i=='dec':
                    candic['dec']=result[4]
                else:
                    candic[i]=None

            alert['candidate']=candic

            alert['prv_candidates']=[{'jd': 2459377.75212963, 'subid': 2792909, 'stackquadid': 2958016, 'diffmaglim': 11.725000381469727, 'program': 'COMMISSIONING', 'candid': 110322760, 'isdiffpos': '1', 'nid': 984, 'quadpos': 3, 'subquadpos': 3, 'field': 809, 'xpos': 745.7625122070312, 'ypos': 141.6396026611328, 'ra': 285.882752, 'dec': 10.3545121, 'magpsf': 10.177000045776367, 'sigmapsf': 0.04899999871850014, 'fwhm': 6.5, 'scorr': 62.47, 'drb': 1.0, 'drbversion': 'modela4.json'}, {'jd': 2459378.75320602, 'subid': 2796549, 'stackquadid': 2961544, 'diffmaglim': 12.064000129699707, 'program': 'COMMISSIONING', 'candid': 110602376, 'isdiffpos': '1', 'nid': 985, 'quadpos': 3, 'subquadpos': 3, 'field': 809, 'xpos': 746.2960205078125, 'ypos': 142.17849731445312, 'ra': 285.8834279, 'dec': 10.3551458, 'magpsf': 10.163000106811523, 'sigmapsf': 0.035999998450279236, 'fwhm': 4.840000152587891, 'scorr': 56.33, 'drb': 1.0, 'drbversion': 'modela4.json'}]
            candid=alert['candid']
            objectID=alert['objectId']

            schema=combine_schemas(glob.glob(f'{os.getcwd()}/*.avsc'))

            avro_packet_path=f"data/real_TURBO_alerts/{candid}_{objectID}.avro"
            out = open(avro_packet_path, "wb")
            fastavro.writer(out, schema, [alert])
            out.close()


#            with open(f"data/real_TURBO_alerts/{candid}_{objectID}.pkl",'wb+') as f:
#                pickle.dump(kdic,f)

#        for i in results[0]:
#            print(i)
        

#        for i results:
#            print(i)

        cur.close()
        conn.close()
        
#        return(kdic)
#        with open('TURBO_dict.pkl','wb+') as f:
#            pickle.dump(kdic,f)
#        print(kdic)

if __name__ == '__main__':
    grab_psql_TURBO()
