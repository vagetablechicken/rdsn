/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef dsn_layer2_TYPES_H
#define dsn_layer2_TYPES_H

#include <dsn/cpp/serialization_helper/dsn_types.h>
#include <iosfwd>

#include <thrift/Thrift.h>
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TTransport.h>

#include <thrift/cxxfunctional.h>

namespace dsn {

struct app_status
{
    enum type
    {
        AS_INVALID = 0,
        AS_AVAILABLE = 1,
        AS_CREATING = 2,
        AS_CREATE_FAILED = 3,
        AS_DROPPING = 4,
        AS_DROP_FAILED = 5,
        AS_DROPPED = 6,
        AS_RECALLING = 7
    };
};

extern const std::map<int, const char *> _app_status_VALUES_TO_NAMES;

class partition_configuration;

class configuration_query_by_index_request;

class configuration_query_by_index_response;

class app_info;

typedef struct _partition_configuration__isset
{
    _partition_configuration__isset()
        : pid(false),
          ballot(false),
          max_replica_count(false),
          primary(false),
          secondaries(false),
          last_drops(false),
          last_committed_decree(false),
          partition_flags(false)
    {
    }
    bool pid : 1;
    bool ballot : 1;
    bool max_replica_count : 1;
    bool primary : 1;
    bool secondaries : 1;
    bool last_drops : 1;
    bool last_committed_decree : 1;
    bool partition_flags : 1;
} _partition_configuration__isset;

class partition_configuration
{
public:
    partition_configuration(const partition_configuration &);
    partition_configuration(partition_configuration &&);
    partition_configuration &operator=(const partition_configuration &);
    partition_configuration &operator=(partition_configuration &&);
    partition_configuration()
        : ballot(0), max_replica_count(0), last_committed_decree(0), partition_flags(0)
    {
    }

    virtual ~partition_configuration() throw();
    ::dsn::gpid pid;
    int64_t ballot;
    int32_t max_replica_count;
    ::dsn::rpc_address primary;
    std::vector<::dsn::rpc_address> secondaries;
    std::vector<::dsn::rpc_address> last_drops;
    int64_t last_committed_decree;
    int32_t partition_flags;

    _partition_configuration__isset __isset;

    void __set_pid(const ::dsn::gpid &val);

    void __set_ballot(const int64_t val);

    void __set_max_replica_count(const int32_t val);

    void __set_primary(const ::dsn::rpc_address &val);

    void __set_secondaries(const std::vector<::dsn::rpc_address> &val);

    void __set_last_drops(const std::vector<::dsn::rpc_address> &val);

    void __set_last_committed_decree(const int64_t val);

    void __set_partition_flags(const int32_t val);

    bool operator==(const partition_configuration &rhs) const
    {
        if (!(pid == rhs.pid))
            return false;
        if (!(ballot == rhs.ballot))
            return false;
        if (!(max_replica_count == rhs.max_replica_count))
            return false;
        if (!(primary == rhs.primary))
            return false;
        if (!(secondaries == rhs.secondaries))
            return false;
        if (!(last_drops == rhs.last_drops))
            return false;
        if (!(last_committed_decree == rhs.last_committed_decree))
            return false;
        if (!(partition_flags == rhs.partition_flags))
            return false;
        return true;
    }
    bool operator!=(const partition_configuration &rhs) const { return !(*this == rhs); }

    bool operator<(const partition_configuration &) const;

    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

    virtual void printTo(std::ostream &out) const;
};

void swap(partition_configuration &a, partition_configuration &b);

inline std::ostream &operator<<(std::ostream &out, const partition_configuration &obj)
{
    obj.printTo(out);
    return out;
}

typedef struct _configuration_query_by_index_request__isset
{
    _configuration_query_by_index_request__isset() : app_name(false), partition_indices(false) {}
    bool app_name : 1;
    bool partition_indices : 1;
} _configuration_query_by_index_request__isset;

class configuration_query_by_index_request
{
public:
    configuration_query_by_index_request(const configuration_query_by_index_request &);
    configuration_query_by_index_request(configuration_query_by_index_request &&);
    configuration_query_by_index_request &operator=(const configuration_query_by_index_request &);
    configuration_query_by_index_request &operator=(configuration_query_by_index_request &&);
    configuration_query_by_index_request() : app_name() {}

    virtual ~configuration_query_by_index_request() throw();
    std::string app_name;
    std::vector<int32_t> partition_indices;

    _configuration_query_by_index_request__isset __isset;

    void __set_app_name(const std::string &val);

    void __set_partition_indices(const std::vector<int32_t> &val);

    bool operator==(const configuration_query_by_index_request &rhs) const
    {
        if (!(app_name == rhs.app_name))
            return false;
        if (!(partition_indices == rhs.partition_indices))
            return false;
        return true;
    }
    bool operator!=(const configuration_query_by_index_request &rhs) const
    {
        return !(*this == rhs);
    }

    bool operator<(const configuration_query_by_index_request &) const;

    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

    virtual void printTo(std::ostream &out) const;
};

void swap(configuration_query_by_index_request &a, configuration_query_by_index_request &b);

inline std::ostream &operator<<(std::ostream &out, const configuration_query_by_index_request &obj)
{
    obj.printTo(out);
    return out;
}

typedef struct _configuration_query_by_index_response__isset
{
    _configuration_query_by_index_response__isset()
        : err(false), app_id(false), partition_count(false), is_stateful(false), partitions(false)
    {
    }
    bool err : 1;
    bool app_id : 1;
    bool partition_count : 1;
    bool is_stateful : 1;
    bool partitions : 1;
} _configuration_query_by_index_response__isset;

class configuration_query_by_index_response
{
public:
    configuration_query_by_index_response(const configuration_query_by_index_response &);
    configuration_query_by_index_response(configuration_query_by_index_response &&);
    configuration_query_by_index_response &operator=(const configuration_query_by_index_response &);
    configuration_query_by_index_response &operator=(configuration_query_by_index_response &&);
    configuration_query_by_index_response() : app_id(0), partition_count(0), is_stateful(0) {}

    virtual ~configuration_query_by_index_response() throw();
    ::dsn::error_code err;
    int32_t app_id;
    int32_t partition_count;
    bool is_stateful;
    std::vector<partition_configuration> partitions;

    _configuration_query_by_index_response__isset __isset;

    void __set_err(const ::dsn::error_code &val);

    void __set_app_id(const int32_t val);

    void __set_partition_count(const int32_t val);

    void __set_is_stateful(const bool val);

    void __set_partitions(const std::vector<partition_configuration> &val);

    bool operator==(const configuration_query_by_index_response &rhs) const
    {
        if (!(err == rhs.err))
            return false;
        if (!(app_id == rhs.app_id))
            return false;
        if (!(partition_count == rhs.partition_count))
            return false;
        if (!(is_stateful == rhs.is_stateful))
            return false;
        if (!(partitions == rhs.partitions))
            return false;
        return true;
    }
    bool operator!=(const configuration_query_by_index_response &rhs) const
    {
        return !(*this == rhs);
    }

    bool operator<(const configuration_query_by_index_response &) const;

    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

    virtual void printTo(std::ostream &out) const;
};

void swap(configuration_query_by_index_response &a, configuration_query_by_index_response &b);

inline std::ostream &operator<<(std::ostream &out, const configuration_query_by_index_response &obj)
{
    obj.printTo(out);
    return out;
}

typedef struct _app_info__isset
{
    _app_info__isset()
        : status(true),
          app_type(false),
          app_name(false),
          app_id(false),
          partition_count(false),
          envs(false),
          is_stateful(false),
          max_replica_count(false),
          expire_second(false),
          create_second(false),
          drop_second(false),
          duplicating(false),
          init_partition_count(true)
    {
    }
    bool status : 1;
    bool app_type : 1;
    bool app_name : 1;
    bool app_id : 1;
    bool partition_count : 1;
    bool envs : 1;
    bool is_stateful : 1;
    bool max_replica_count : 1;
    bool expire_second : 1;
    bool create_second : 1;
    bool drop_second : 1;
    bool duplicating : 1;
    bool init_partition_count : 1;
} _app_info__isset;

class app_info
{
public:
    app_info(const app_info &);
    app_info(app_info &&);
    app_info &operator=(const app_info &);
    app_info &operator=(app_info &&);
    app_info()
        : status((app_status::type)0),
          app_type(),
          app_name(),
          app_id(0),
          partition_count(0),
          is_stateful(0),
          max_replica_count(0),
          expire_second(0),
          create_second(0),
          drop_second(0),
          duplicating(0),
          init_partition_count(-1)
    {
        status = (app_status::type)0;
    }

    virtual ~app_info() throw();
    app_status::type status;
    std::string app_type;
    std::string app_name;
    int32_t app_id;
    int32_t partition_count;
    std::map<std::string, std::string> envs;
    bool is_stateful;
    int32_t max_replica_count;
    int64_t expire_second;
    int64_t create_second;
    int64_t drop_second;
    bool duplicating;
    int32_t init_partition_count;

    _app_info__isset __isset;

    void __set_status(const app_status::type val);

    void __set_app_type(const std::string &val);

    void __set_app_name(const std::string &val);

    void __set_app_id(const int32_t val);

    void __set_partition_count(const int32_t val);

    void __set_envs(const std::map<std::string, std::string> &val);

    void __set_is_stateful(const bool val);

    void __set_max_replica_count(const int32_t val);

    void __set_expire_second(const int64_t val);

    void __set_create_second(const int64_t val);

    void __set_drop_second(const int64_t val);

    void __set_duplicating(const bool val);

    void __set_init_partition_count(const int32_t val);

    bool operator==(const app_info &rhs) const
    {
        if (!(status == rhs.status))
            return false;
        if (!(app_type == rhs.app_type))
            return false;
        if (!(app_name == rhs.app_name))
            return false;
        if (!(app_id == rhs.app_id))
            return false;
        if (!(partition_count == rhs.partition_count))
            return false;
        if (!(envs == rhs.envs))
            return false;
        if (!(is_stateful == rhs.is_stateful))
            return false;
        if (!(max_replica_count == rhs.max_replica_count))
            return false;
        if (!(expire_second == rhs.expire_second))
            return false;
        if (!(create_second == rhs.create_second))
            return false;
        if (!(drop_second == rhs.drop_second))
            return false;
        if (__isset.duplicating != rhs.__isset.duplicating)
            return false;
        else if (__isset.duplicating && !(duplicating == rhs.duplicating))
            return false;
        if (!(init_partition_count == rhs.init_partition_count))
            return false;
        return true;
    }
    bool operator!=(const app_info &rhs) const { return !(*this == rhs); }

    bool operator<(const app_info &) const;

    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

    virtual void printTo(std::ostream &out) const;
};

void swap(app_info &a, app_info &b);

inline std::ostream &operator<<(std::ostream &out, const app_info &obj)
{
    obj.printTo(out);
    return out;
}

} // namespace

#endif
