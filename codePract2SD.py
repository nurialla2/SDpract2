import pywren_ibm_cloud as pywren
from functools import wraps
import pickle
import time

COS_BUCKET = 'bucket-name'
N_SLAVES = 100 #mai serà més de 100
X = float(0.100)

elapsed_time = 0.0

def timeit(fn):
    @wraps(fn)
    def with_profiling(*args, **kwargs):
        global elapsed_time
        start_time = time.time()
        ret = fn(*args, **kwargs)
        elapsed_time = time.time() - start_time
        return ret
    return with_profiling

def master(id, x, ibm_cos):
    write_permission_list = []
    dicc = {}
    list = []
# 1. monitor COS bucket each X seconds
    time.sleep(X)
    contingut = None
# 2. List all "p_write_{id}" files
    var = ibm_cos.list_objects_v2(Bucket=COS_BUCKET, Prefix='p_write_')['KeyCount']
    while var == 0:
        time.sleep(1)
        var = ibm_cos.list_objects_v2(Bucket=COS_BUCKET, Prefix='p_write_')['KeyCount']
    while var != 0:
        pp = ibm_cos.list_objects(Bucket=COS_BUCKET, Prefix='p_write_')
        contingut = pp.get('Contents')
        time.sleep(X)
        if contingut != None:
            i = 0
            while i < len(contingut):
                dicc = {}
                x = contingut[i].get('Key')
                y = contingut[i].get('LastModified')
                dicc['Key'] = x
                dicc['LastModified'] = y
                list.append(dicc)
                i = i+1
        # 3. Order objects by time of creation
            list.sort(key=lambda x: x['LastModified'])
        # 4. Pop first object of the list "p_write_{id}"
            z = 0
            total = len(list)
            while z < total:
        # 5. Write empty "write_{id}" object into COS
                element = list.pop(0)
                clau = element.get('Key')
                valor1 = clau.split("{")
                tot = valor1[1].split("}")
                ident = tot[0]
                nomproc = "write_{" + str(ident) + "}"
                ibm_cos.put_object(Bucket=COS_BUCKET,Key=nomproc)
        # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
                bor = "p_write_{" + str(ident) + "}"
                ibm_cos.delete_object(Bucket=COS_BUCKET, Key=bor)
                write_permission_list.append(int(ident))
        # 7. Monitor "result.json" object each X seconds until it is updated
                trobat = False
                while trobat == False:
                    time.sleep(X)
                    var = ibm_cos.list_objects_v2(Bucket=COS_BUCKET, Prefix="result.txt")['KeyCount'] 
                    while var == 0:
                        time.sleep(X)
                        var = ibm_cos.list_objects_v2(Bucket=COS_BUCKET, Prefix="result.txt")['KeyCount'] 
                    objecte = ibm_cos.get_object(Bucket=COS_BUCKET, Key="result.txt")['Body'].read()
                    contingut = pickle.loads(objecte)
                    val = contingut[-1:]
                    if val[0] == int(ident):
                        trobat = True
        # 8. Delete from COS “write_{id}”
                ibm_cos.delete_object(Bucket=COS_BUCKET, Key=nomproc)
                z = z + 1
        var = ibm_cos.list_objects_v2(Bucket=COS_BUCKET, Prefix='p_write_')['KeyCount']
                
# 8. Back to step 1 until no "p_write_{id}" objects in the bucket

    return write_permission_list

def slave(id, x, ibm_cos):

# 1. Write empty "p_write_{id}" object into COS
    
    nombre = "p_write_{" + str(id) + "}"
    z = pickle.dumps(nombre)
    ibm_cos.put_object(Bucket=COS_BUCKET,Key=nombre)
# 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    nombre2 = "write_{" + str(id) + "}"
    var = ibm_cos.list_objects_v2(Bucket=COS_BUCKET, Prefix=nombre2)['KeyCount']
    while var == 0:
        time.sleep(X)
        var = ibm_cos.list_objects_v2(Bucket=COS_BUCKET, Prefix=nombre2)['KeyCount']
# 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    var = ibm_cos.list_objects_v2(Bucket=COS_BUCKET, Prefix="result.txt")['KeyCount']
    if var == 0:
        contingut = [id]
    else:
        objecte = ibm_cos.get_object(Bucket=COS_BUCKET, Key="result.txt")['Body'].read()
        contingut = pickle.loads(objecte)
        contingut.append(id)
    z = pickle.dumps(contingut)
    ibm_cos.put_object(Bucket=COS_BUCKET,Key="result.txt", Body=z)
# 4. Finish
# No need to return anything

if __name__ == '__main__':
    i = 0
    trobat = False
    if (N_SLAVES >100):
        N_SLAVES = 100
    if (N_SLAVES == 0):
        N_SLAVES = 1
    pw = pywren.ibm_cf_executor()
    ibm_cos = pw.internal_storage.get_client()
    start_time = time.time()
    pw.map(slave, range(N_SLAVES))
    pw.call_async(master, 0)
    write_permission_list = pw.get_result()
    elapsed_time = time.time() - start_time
    print ('Elapsed time:{0:.2f}'.format(elapsed_time))
    print("Write_permission_list:", write_permission_list)
    resultfin = ibm_cos.get_object(Bucket=COS_BUCKET, Key='result.txt')['Body'].read()
    llista = pickle.loads(resultfin)
    print("Result.txt:", llista)
    for i in range(len(write_permission_list)):
        if write_permission_list[i] != llista[i]:
            trobat = True

    if trobat == True:
        print("No s'ha fet bé")
    else:
        print("S'ha fet correctament")

    llista.sort()
    print("Result.txt ordenat:", llista)
    ibm_cos.delete_object(Bucket=COS_BUCKET, Key="result.txt") #borrar fitxer 
    pw.clean() #borrar dades temporals
