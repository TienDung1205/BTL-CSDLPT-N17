import mysql.connector
import math
import traceback

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

def getopenconnection(user='root', password='123456', dbname='mysql'):
    return mysql.connector.connect(
        host='localhost',
        user=user,
        password=password,
        database=dbname
    )

# SETUP Functions
def createdb(dbname):
    """
    We create a DB by connecting to the default user and database of MySQL
    The function first checks if an existing database exists for a given name, else creates it.
    :return: None
    """
    # Connect to the default database
    con = getopenconnection()
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = %s", (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named "{0}" already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def delete_db(dbname):
    con = getopenconnection(dbname='mysql')
    cur = con.cursor()
    cur.execute('DROP DATABASE IF EXISTS %s' % (dbname,))
    cur.close()
    con.close()

def deleteAllPublicTables(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("DROP TABLE IF EXISTS %s" % (tablename,))
    openconnection.commit()
    cur.close()


####### Tester support
def getCountrangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Get number of rows for each partition
    :param ratingstablename:
    :param numberofpartitions:
    :param openconnection:
    :return:
    """
    cur = openconnection.cursor()
    countList = []
    interval = 5.0 / numberofpartitions
    cur.execute("SELECT COUNT(*) FROM %s WHERE rating >= %s AND rating <= %s" % (ratingstablename, 0, interval))
    countList.append(int(cur.fetchone()[0]))

    lowerbound = interval
    for i in range(1, numberofpartitions):
        cur.execute("SELECT COUNT(*) FROM %s WHERE rating > %s AND rating <= %s" % (ratingstablename, lowerbound, lowerbound + interval))
        lowerbound += interval
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList

def getCountroundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    '''
    Get number of rows for each partition
    :param ratingstablename:
    :param numberofpartitions:
    :param openconnection:
    :return:
    '''
    cur = openconnection.cursor()
    countList = []
    for i in range(0, numberofpartitions):
        cur.execute(
            "SELECT COUNT(*) FROM (SELECT ROW_NUMBER() OVER () AS rnum, * FROM %s) AS temp WHERE (rnum-1) %% %s = %s" % (
                ratingstablename, numberofpartitions, i))
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList

# Helpers for Tester functions
def checkpartitioncount(cursor, expectedpartitions, prefix):
    cursor.execute(
        "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE %s" % (prefix + '%',))
    count = int(cursor.fetchone()[0])
    if count != expectedpartitions:
        raise Exception(
            'Range partitioning not done properly. Expected %s table(s) but found %s table(s)' % (
                expectedpartitions, count))

def totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex):
    selects = []
    for i in range(partitionstartindex, n + partitionstartindex):
        selects.append('SELECT * FROM %s%s' % (rangepartitiontableprefix, i))
    cur.execute('SELECT COUNT(*) FROM (%s) AS T' % (' UNION ALL '.join(selects)))
    count = int(cur.fetchone()[0])
    return count

def testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    cur = openconnection.cursor()
    if not isinstance(n, int) or n < 0:
        # Test 1: Check the number of tables created, if 'n' is invalid
        checkpartitioncount(cur, 0, rangepartitiontableprefix)
    else:
        # Test 2: Check the number of tables created, if all args are correct
        checkpartitioncount(cur, n, rangepartitiontableprefix)

        # Test 3: Test Completeness by SQL UNION ALL Magic
        count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
        if count < ACTUAL_ROWS_IN_INPUT_FILE:
            raise Exception(
                "Completeness property of Partitioning failed. Expected %s rows after merging all tables, but found %s rows" % (
                    ACTUAL_ROWS_IN_INPUT_FILE, count))

        # Test 4: Test Disjointness by SQL UNION Magic
        count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
        if count > ACTUAL_ROWS_IN_INPUT_FILE:
            raise Exception(
                "Disjointness property of Partitioning failed. Expected %s rows after merging all tables, but found %s rows" % (
                    ACTUAL_ROWS_IN_INPUT_FILE, count))

        # Test 5: Test Reconstruction by SQL UNION Magic
        count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
        if count != ACTUAL_ROWS_IN_INPUT_FILE:
            raise Exception(
                "Reconstruction property of Partitioning failed. Expected %s rows after merging all tables, but found %s rows" % (
                    ACTUAL_ROWS_IN_INPUT_FILE, count))
    cur.close()

def testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
    cur = openconnection.cursor()
    cur.execute(
        'SELECT COUNT(*) FROM %s WHERE %s = %s AND %s = %s AND %s = %s' % (
            expectedtablename, USER_ID_COLNAME, userid, MOVIE_ID_COLNAME, itemid, RATING_COLNAME, rating))
    count = int(cur.fetchone()[0])
    cur.close()
    if count != 1:
        return False
    return True

def testEachRangePartition(ratingstablename, n, openconnection, rangepartitiontableprefix):
    countList = getCountrangepartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(0, n):
        cur.execute("SELECT COUNT(*) FROM %s%s" % (rangepartitiontableprefix, i))
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception("%s%s has %s of rows while the correct number should be %s" % (
                rangepartitiontableprefix, i, count, countList[i]))
    cur.close()

def testEachRoundrobinPartition(ratingstablename, n, openconnection, roundrobinpartitiontableprefix):
    countList = getCountroundrobinpartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(0, n):
        cur.execute("SELECT COUNT(*) FROM %s%s" % (roundrobinpartitiontableprefix, i))
        count = cur.fetchone()[0]
        if count != countList[i]:
            raise Exception("%s%s has %s of rows while the correct number should be %s" % (
                roundrobinpartitiontableprefix, i, count, countList[i]))
    cur.close()

# ##########

def testloadratings(MyAssignment, ratingstablename, filepath, openconnection, rowsininpfile):
    """
    Tests the load ratings function
    :param ratingstablename: Argument for function to be tested
    :param filepath: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param rowsininpfile: Number of rows in the input file provided for assertion
    :return: Raises exception if any test fails
    """
    try:
        MyAssignment.loadratings(ratingstablename, filepath, openconnection)
        # Test 1: Count the number of rows inserted
        cur = openconnection.cursor()
        cur.execute('SELECT COUNT(*) FROM %s' % (ratingstablename,))
        count = int(cur.fetchone()[0])
        cur.close()
        if count != rowsininpfile:
            raise Exception(
                'Expected %s rows, but %s rows in "%s" table' % (rowsininpfile, count, ratingstablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testrangepartition(MyAssignment, ratingstablename, n, openconnection, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    """
    Tests the range partition function for Completeness, Disjointness and Reconstruction
    :param ratingstablename: Argument for function to be tested
    :param n: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param partitionstartindex: Indicates how the table names are indexed. Do they start as rangepart1, 2 ... or rangepart0, 1, 2...
    :return: Raises exception if any test fails
    """
    try:
        MyAssignment.rangepartition(ratingstablename, n, openconnection)
        testrangeandrobinpartitioning(n, openconnection, RANGE_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRangePartition(ratingstablename, n, openconnection, RANGE_TABLE_PREFIX)
        return [True, None]
    except Exception as e:
        traceback.print_exc()
        return [False, e]

def testroundrobinpartition(MyAssignment, ratingstablename, numberofpartitions, openconnection,
                            partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    """
    Tests the round robin partitioning for Completeness, Disjointness and Reconstruction
    :param ratingstablename: Argument for function to be tested
    :param numberofpartitions: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param robinpartitiontableprefix: This function assumes that you tables are named in an order. Eg: robinpart1, robinpart2...
    :return: Raises exception if any test fails
    """
    try:
        MyAssignment.roundrobinpartition(ratingstablename, numberofpartitions, openconnection)
        testrangeandrobinpartitioning(numberofpartitions, openconnection, RROBIN_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRoundrobinPartition(ratingstablename, numberofpartitions, openconnection, RROBIN_TABLE_PREFIX)
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testroundrobininsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    """
    Tests the roundrobin insert function by checking whether the tuple is inserted in the Expected table you provide
    :param ratingstablename: Argument for function to be tested
    :param userid: Argument for function to be tested
    :param itemid: Argument for function to be tested
    :param rating: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param expectedtableindex: The expected table to which the record has to be saved
    :return: Raises exception if any test fails
    """
    try:
        expectedtablename = RROBIN_TABLE_PREFIX + expectedtableindex
        MyAssignment.roundrobininsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Round robin insert failed! Couldn\'t find (%s, %s, %s) tuple in %s table' % (userid, itemid, rating,
                                                                                              expectedtablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testrangeinsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    """
    Tests the range insert function by checking whether the tuple is inserted in the Expected table you provide
    :param ratingstablename: Argument for function to be tested
    :param userid: Argument for function to be tested
    :param itemid: Argument for function to be tested
    :param rating: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param expectedtableindex: The expected table to which the record has to be saved
    :return: Raises exception if any test fails
    """
    try:
        expectedtablename = RANGE_TABLE_PREFIX + expectedtableindex
        MyAssignment.rangeinsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Range insert failed! Couldn\'t find (%s, %s, %s) tuple in %s table' % (userid, itemid, rating,
                                                                                        expectedtablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]