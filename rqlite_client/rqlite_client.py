import pyrqlite.dbapi2 as dbapi2
import config


def get_connection():
    connection = dbapi2.connect(
        host=config.RQLITE_HOST,
        port=config.RQLITE_PORT
    )

    return connection


def get_strategy():
    try:
        sql = "select current_strategy from aggregate"
        connection = get_connection()
        with connection.cursor() as cursor:

            cursor.execute(sql)
            result = cursor.fetchone()
            strategy = result[0]

            return strategy




    finally:
        connection.close()


def insert_host(name, ip, max_cpu, max_mem, capacity):
    connection = get_connection()

    insert = "INSERT INTO hosts(host_identifier, ip, max_cpu, max_mem, current_cpu, current_mem, disk_pool_capacity) VALUES(?, ?, ?, ?, ?, ?, ?)"

    data = (name, ip, max_cpu, max_mem, max_cpu, max_mem, capacity)

    try:
        with connection.cursor() as cursor:
            r = cursor.execute(insert, data)
            return r.lastrowid
    finally:
        connection.close()


def update_host_resources(id, cpu_count, mem_count, capacity):
    connection = get_connection()

    update = "UPDATE hosts SET current_cpu = {}, current_mem = {}, disk_pool_capacity = {} WHERE id = {}".format(
        cpu_count, mem_count, capacity, id)

    try:
        with connection.cursor() as cursor:
            cursor.execute(update)

    finally:
        connection.close()


def get_hosts():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM hosts"
            cursor.execute(sql)
            results = cursor.fetchall()
            if results is None:
                return None
            else:
                ret = []
                for result in results:
                    ret.append(result)
                return ret

    finally:
        connection.close()


def get_host_resources():
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT max(current_cpu) as max_cpu, max(current_mem) as max_mem, sum(max_cpu) as max_total_cpu, sum(max_mem) as max_total_mem, sum(current_cpu) as total_cpu, sum(current_mem) as total_mem, max(disk_pool_capacity) as disk_pool_capacity FROM hosts"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result is None:
                return None
            return result

    finally:
        connection.close()


def get_host(id):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM hosts WHERE id = {}".format(id)
            cursor.execute(sql)
            result = cursor.fetchone()
            if result is None:
                return None
            return result

    finally:
        connection.close()


def get_host_by_uuid(uuid):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM hosts WHERE  host_identifier = '{}'".format(uuid)
            cursor.execute(sql)
            result = cursor.fetchone()
            if result is None:
                return None
            return result

    finally:
        connection.close()


def insert_cluster(cpu_count, mem_count, max_cpu, max_mem, contig_cpu, contig_mem, disk_pool_capacity, strategy):
    connection = get_connection()

    insert = "INSERT INTO cluster(cpu_count, mem_count, max_cpu, max_mem, contig_cpu, contig_mem, disk_pool_capacity, strategy) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"

    data = (cpu_count, mem_count, max_cpu, max_mem, contig_cpu, contig_mem, disk_pool_capacity, strategy)

    try:
        with connection.cursor() as cursor:
            r = cursor.execute(insert, data)
            return r.lastrowid
    finally:
        connection.close()


def update_cluster(id, cpu_count, mem_count, max_cpu, max_mem, contig_cpu, contig_mem, disk_pool_capacity):
    connection = get_connection()

    insert = "UPDATE cluster SET cpu_count=?, mem_count= ?, max_cpu= ?, max_mem= ?, contig_cpu= ?, contig_mem= ?, disk_pool_capacity= ? where id= ?"

    data = (cpu_count, mem_count, max_cpu, max_mem, contig_cpu, contig_mem, disk_pool_capacity, id)

    try:
        with connection.cursor() as cursor:
            cursor.execute(insert, data)

    finally:
        connection.close()


def get_cluster(id):
    connection = dbapi2.connect(
        host='192.168.0.122',
        port=31426,
    )

    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM cluster WHERE id = {}".format(id)
            cursor.execute(sql)
            result = cursor.fetchone()
            if result is None:
                return None
            return result

    finally:
        connection.close()

def get_image(name) :
    try:
        sql = "select * from images where name = '{}'".format(name)
        connection = dbapi2.connect(
            host='192.168.0.122',
            port=31426,
        )
        with connection.cursor() as cursor:

            cursor.execute(sql)
            result = cursor.fetchone()
            image = result[1]
            arch = result[2]
            machine = result[3]
            return image, arch, machine




    finally:
        connection.close()


def get_agg_clusters(strategy):
    if strategy == 'pack':
        sql = "select * from aggregate_cluster order by max_contig_mem asc"
    else:
        sql = "select * from aggregate_cluster order by max_contig_mem desc"

    cluster_list = []
    try:
        connection = get_connection()
        with connection.cursor() as cursor:

            cursor.execute(sql)
            results = cursor.fetchall()
            for result in results:
                cluster = {}
                cluster['name'] = result[0]
                cluster['scheduling_url'] = result[1]
                cluster['scheduling_topic'] = result[2]
                cluster['max_contig_cpu'] = result[9]
                cluster['max_contig_mem'] = result[10]
                cluster['available_disk'] = result[8]
                cluster['strategy'] = result[11]
                cluster['closed'] = result[12]
                cluster_list.append(cluster)
            return cluster_list

    finally:
        connection.close()


def create_agg_cluster(cluster):
    sql = "INSERT INTO aggregate_cluster (name, scheduling_url, scheduling_topic, max_cpu, max_mem, max_disk, avail_cpu, avail_mem, avail_disk, max_contig_cpu, max_contig_mem, cluster_strategy, closed, reconcile) VALUES ('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, '{}', {}, {});".format(
        cluster['name'], cluster['scheduling_url'], cluster['scheduling_topic'], cluster['max_cpu'], cluster['max_mem'],
        cluster['max_disk'], cluster['avail_cpu'], cluster['avail_mem'], cluster['avail_disk'],
        cluster['max_contig_cpu'], cluster['max_contig_mem'], cluster['cluster_strategy'],
        cluster['closed'], cluster['reconcile'])
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(sql)

    finally:
        connection.close()


def get_agg_cluster(name):
    sql = "select * from aggregate_cluster where name = '{}'".format(name)

    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            if result is None:
                return result
            cluster = {}
            cluster['name'] = result[0]
            cluster['scheduling_url'] = result[1]
            cluster['scheduling_topic'] = result[2]
            cluster['max_cpu'] = result[3]
            cluster['max_mem'] = result[4]
            cluster['max_disk'] = result[5]
            cluster['available_cpu'] = result[6]
            cluster['available_mem'] = result[7]
            cluster['available_disk'] = result[8]
            cluster['max_contig_cpu'] = result[9]
            cluster['max_contig_mem'] = result[10]
            cluster['strategy'] = result[11]
            cluster['closed'] = result[12]
            cluster['reconcile'] = result[13]

            return cluster

    finally:
        connection.close()


def get_agg_cluster_resources(cluster_name):
    select = 'select sum(cpu) as cpu, sum(mem) as mem, max(cpu) as max_cpu, max(mem) as max_mem from aggregate_hosts where cluster_name = "{}";'.format(
        cluster_name)
    try:

        connection = get_connection()
        with connection.cursor() as cursor:

            cursor.execute(select)
            result = cursor.fetchone()
            cpu = result[0]
            mem = result[1]
            max_cpu = result[2]
            max_mem = result[3]

            return cpu, mem, max_cpu, max_mem

    finally:
        connection.close()


def open_agg_cluster(name):
    update = 'UPDATE aggregate_hosts SET closed = 0 where name = "{}";'.format(name)

    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(update)

    finally:
        connection.close()


def close_agg_cluster(name):
    update = 'UPDATE aggregate_hosts SET closed = 1 where name = "{}";'.format(name)

    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(update)

    finally:
        connection.close()


def update_agg_cluster_resources(name, cpu, mem, disk, max_cpu, max_mem):
    update = 'UPDATE aggregate_hosts SET max_contig_mem avail_cpu = {}, avail_mem = {}, avail_disk = {}, max_contig_cpu = {}, max_contig_mem = {} WHERE name = "{}"'.format(
        cpu, mem, disk, max_cpu, max_mem, name)

    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(update)

    finally:
        connection.close()


def get_agg_hosts(cluster_name, strategy):
    if strategy == 'pack':
        sql = 'select * from aggregate_hosts where cluster_name = "{}" order by mem asc'.format(cluster_name)
    else:
        sql = 'select * from aggregate_hosts where cluster_name = "{}" order by mem desc'.format(cluster_name)

    host_list = []
    try:
        connection = get_connection()
        with connection.cursor() as cursor:

            cursor.execute(sql)
            results = cursor.fetchall()
            for result in results:
                host = {}
                host['uuid'] = result[0]
                host['cpu'] = result[1]
                host['mem'] = result[2]
                host_list.append(host)
            return host_list

    finally:
        connection.close()


def update_agg_host_resources(uuid, cpu, mem):
    update = 'UPDATE aggregate_hosts SET cpu = {}, mem = {} WHERE uuid = "{}"'.format(cpu, mem, uuid)

    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(update)

    finally:
        connection.close()