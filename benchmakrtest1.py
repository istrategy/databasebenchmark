import json
import mysql.connector
import time

class benchmark:
    datafilename = "data.txt"

    def myconn(self):
        myconn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="benchmark"
        )
        return myconn


    def saveData(self,data):
        mydf = open(self.datafilename,'a')
        mydf.write(data + "\n")
        mydf.close()


    def displayJson(self, filename):
        # Opening JSON file
        f = open(filename, )
        # returns JSON object as
        # a dictionary
        data = json.load(f)
        # Iterating through the json
        # list
        for (k, v) in data.items():
            print("Key: " + k)
            print("Value: " + str(v))

        # Closing file
        f.close()


    def insertTestRec(self):
        conn = self.myconn()
        print(conn)
        sql = "INSERT INTO student (id, name, address) VALUES (null, %s, %s)"
        val = ("John", "Highway 21")
        mycursor = conn.cursor()
        mycursor.execute(sql, val)
        conn.commit()

    def insertManyRec(self, trecArray):
        conn = self.myconn()
        for trec in trecArray:
            # print(conn)
            sql = "INSERT INTO student (id, name, address, latitude,longitude) VALUES (null, %s, %s, %s, %s)"
            val = (trec['name'], trec['address'], trec['latitude'], trec['longitude'])
            mycursor = conn.cursor()
            mycursor.execute(sql, val)
        conn.commit()
        # print(trec)

    def insertOneRecPS(self, trec): # prepared statements
        conn = self.myconn()
        # print(conn)
        sql = "INSERT INTO student (id, name, address, latitude,longitude) VALUES (null, %s, %s, %s, %s)"
        val = ( trec['name'],trec['address'],trec['latitude'],trec['longitude'])
        mycursor = conn.cursor()
        mycursor.execute(sql, val)
        conn.commit()
        # print(trec)

    def insertOneRecNPS(self, trec): # non preapared statements
        conn = self.myconn()
        # print(conn)
        sql = "INSERT INTO student (id, name, address, latitude,longitude) VALUES (null,\"" + trec['name'] + "\",\""  + trec['address'] + "\",\"" + trec['latitude'] + "\",\"" + trec['longitude'] + "\" )"
        # print(sql)
        # val = ( trec['name'],trec['address'],trec['latitude'],trec['longitude'])
        mycursor = conn.cursor()
        mycursor.execute(sql)
        conn.commit()
        # print(trec)


    def insertManyrecs(self, filename,type):
        print("Insert "+ type+ " " + filename)
        self.saveData("Insert "+ type+ " " + filename)

        #sart timecounter
        f = open(filename, )
        # returns JSON object as
        # a dictionary
        data = json.load(f)
        # Iterating through the json
        # list
        tic = time.perf_counter()
        for (k, v) in data.items():
            # print("Key: " + k)
            # print("Value: " + str(v))
            if type == "PS":
                self.insertOneRecNPS(v)
            else:
                self.insertOneRecPS(v)

        toc = time.perf_counter()
        print(f"Running time {toc - tic:0.4f} seconds")
        self.saveData(f"Running time {toc - tic:0.4f} seconds")

        # Closing file
        f.close()

    def insertManyrecsTransactions(self, filename, tsize):
        print("Insert " + filename)
        #sart timecounter
        f = open(filename, )
        # returns JSON object as
        # a dictionary
        data = json.load(f)
        # Iterating through the json
        # list
        tic = time.perf_counter()
        batchArray = []
        tcounter = 0
        for (k, v) in data.items():
            # print("Key: " + k)
            # print("Value: " + str(v))
            batchArray.append(v)
            if tcounter < tsize:
                self.insertManyRec(batchArray)
                batchArray = []
                tcounter = 0
            tcounter = tcounter + 1
        #
        self.insertManyRec(batchArray)

        toc = time.perf_counter()
        print(f"Running time {toc - tic:0.4f} seconds")
        # Closing file
        f.close()



bm = benchmark()
# filename = "students100.json"
# bm.displayJson(filename)

# bm.insertTestRec()

# test 1 insert 100 recs
filename = "students1K.json"

bm.saveData("")
bm.insertManyrecs(filename,"PS")
bm.saveData("")
bm.insertManyrecs(filename,"NPS")

transactionSize =10

# bm.insertManyrecsTransactions(filename, transactionSize)


