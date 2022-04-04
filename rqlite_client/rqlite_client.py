import pyrqlite.dbapi2 as dbapi2


def get_connection(host, port):
    connection = dbapi2.connect(
        host=host,
        port=port
    )

    return connection


def execute_sql(host, port, sql):
    try:
        connection = get_connection(host, port)
        with connection.cursor() as cursor:
            cursor.execute(sql)

    finally:
        connection.close()


def execute_query_many(host, port, sql):
    try:

        connection = get_connection(host, port)
        with connection.cursor() as cursor:

            cursor.execute(sql)
            results = cursor.fetchall()
            return results

    finally:
        connection.close()


def execute_query_one(host, port, sql):
    try:

        connection = get_connection(host, port)
        with connection.cursor() as cursor:

            cursor.execute(sql)
            results = cursor.fetchone()
            return results

    finally:
        connection.close()


def create_agg_cluster(host, port, name, scheduling_url, scheduling_topic, cluster_strategy, storage_pool, closed,
                       reconcile):
    insert = "INSERT INTO aggregate_cluster (name, scheduling_url, scheduling_topic, cluster_strategy, storage_pool, closed, reconcile) VALUES ('{}', '{}', '{}', '{}', '{}', {}, {});".format(
        name, scheduling_url, scheduling_topic, cluster_strategy, storage_pool, closed, reconcile)
    execute_sql(host, port, insert)


def get_strategy(host, port, cluster_name):
    select = "select cluster_strategy from aggregate_cluster where name = '{}'".format(cluster_name)
    execute_query_one(host, port, select)


def get_cluster_resources(host, port, strategy):
    if strategy == "pack":
        select = "select * from cluster_resources order by max_contig_mem desc"
    elif strategy == "spread":
        select = "select * from cluster_resources order by max_contig_mem asc"
    else:
        return

    execute_query_many(host, port, select)


def get_agg_cluster(host, port, cluster_name):
    select = "select * from aggregate_cluster where name = '{}'".format(cluster_name)

    execute_query_many(host, port, select)


def get_host_resources(host, port, cluster_name, strategy):
    if strategy == "pack":
        select = "select * from host_resources where cluster_name = '{}' order by memcount desc".format(cluster_name)
    elif strategy == "spread":
        select = "select * from host_resources where cluster_name = '{}' order by memcount asc".format(cluster_name)
    else:
        return

    execute_query_many(host, port, select)


def get_host(host, port, host_id):
    select = "selct * from hosts where id = {}".format(host_id)
    execute_query_one(host, port, select)


def get_cpus(host, port, host_id):
    select = "select * from cpu where host_id = {}".format(host_id)
    execute_query_many(host, port, select)


def get_mems(host, port, host_id):
    select = "select * from mem where host_id = {}".format(host_id)
    execute_query_many(host, port, select)


def clear_cpus_for_host(host, port, host_id):
    delete = "delete from cpu where host_id = {}".format(host_id)
    execute_sql(host, port, delete)


def clear_mems_for_host(host, port, host_id):
    delete = "delete from mem where host_id = {}".format(host_id)
    execute_sql(host, port, delete)


def create_cpu_resource(host, port, host_id, cluster_name):
    insert = "INSERT INTO cpu (host_id, host_count, cluster_name) VALUES ({}, 1, '{}');".format(host_id, cluster_name)
    execute_sql(host, port, insert)


def insert_host(host, port, host_identifier, ip, cpu, mem, disk):
    insert = "INSERT INTO hosts(host_identifier, ip, cpu, mem, disk_pool) VALUES('{}', '{}', {}, {}, {})".format(
        host_identifier, ip, cpu, mem, disk)
    execute_sql(host, port, insert)


def update_host(host, port, id, cpu_count, mem_count, capacity):
    update = "UPDATE hosts SET current_cpu = {}, current_mem = {}, disk_pool_capacity = {} WHERE id = {}".format(
        cpu_count, mem_count, capacity, id)
    execute_sql(host, port, update)


def get_host(host, port, id):
    sql = "SELECT * FROM hosts WHERE id = {}".format(id)
    execute_query_one(host, port, sql)


def get_hosts(host, port):
    sql = "SELECT * FROM hosts"
    execute_query_many(host, port, sql)


def get_host_by_uuid(host, port, uuid):
    sql = "SELECT * FROM hosts WHERE  host_identifier = '{}'".format(uuid)
    execute_query_one(host, port, sql)
