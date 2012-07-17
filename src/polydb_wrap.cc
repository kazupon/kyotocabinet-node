/*
 * polydb wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include "polydb_wrap.h"
#include <assert.h>
#include "debug.h"
#include <kcdbext.h>
#include <kcdb.h>

using namespace v8;
namespace kc = kyotocabinet;


#define DEFINE_FUNC(Name, Type)                                             \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                   \
    HandleScope scope;                                                      \
                                                                            \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());          \
    assert(obj != NULL);                                                    \
                                                                            \
    if ((args.Length() == 1 && (!args[0]->IsObject())                       \
                              | !args[0]->IsFunction())) {                  \
      ThrowException(Exception::TypeError(String::New("Bad argument")));    \
      return args.This();                                                   \
    }                                                                       \
                                                                            \
    kc_req_t *req = (kc_req_t *)malloc(sizeof(kc_req_t));                   \
    req->type = Type;                                                       \
    req->result = PolyDB::Error::SUCCESS;                                   \
    req->wrapdb = obj;                                                      \
    req->cb.Clear();                                                        \
                                                                            \
    if (args.Length() > 0 && args[0]->IsFunction()) {                       \
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0])); \
    }                                                                       \
                                                                            \ 
    uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));             \
    uv_req->data = req;                                                     \
                                                                            \
    int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone); \
    TRACE("uv_queue_work: ret=%d\n", ret);                                  \
                                                                            \
    obj->Ref();                                                             \
    return args.This();                                                     \
  }                                                                         \

#define DEFINE_CHAR_PARAM_FUNC(Name, Type)                                            \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                             \
    HandleScope scope;                                                                \
                                                                                      \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());                    \
    assert(obj != NULL);                                                              \
                                                                                      \
    if ( (args.Length() == 0) ||                                                      \
         (args.Length() == 1 && (!args[0]->IsString()) | !args[0]->IsFunction()) ) {  \
      ThrowException(Exception::TypeError(String::New("Bad argument")));              \
      return args.This();                                                             \
    }                                                                                 \
                                                                                      \
    kc_char_cmn_req_t *req = (kc_char_cmn_req_t *)malloc(sizeof(kc_char_cmn_req_t));  \
    req->type = Type;                                                                 \
    req->result = PolyDB::Error::SUCCESS;                                             \
    req->wrapdb = obj;                                                                \
    req->cb.Clear();                                                                  \
    req->str = NULL;                                                                  \
                                                                                      \
    if (args.Length() == 1) {                                                         \
      if (args[0]->IsFunction()) {                                                    \
        req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));         \
      } else if (args[0]->IsString()) {                                               \
        String::Utf8Value str(args[0]->ToString());                                   \
        req->str = kc::strdup(*str);                                                  \
      }                                                                               \
    } else {                                                                          \
      String::Utf8Value str(args[0]->ToString());                                     \
      req->str = kc::strdup(*str);                                                    \
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));           \
    }                                                                                 \
                                                                                      \
    uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));                       \
    uv_req->data = req;                                                               \
                                                                                      \
    int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);           \
    TRACE("uv_queue_work: ret=%d\n", ret);                                            \
                                                                                      \
    obj->Ref();                                                                       \
    return args.This();                                                               \
  }                                                                                   \

#define DEFINE_BOOL_PARAM_FUNC(Name, Method, INIT_VALUE)                                                                \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                                                               \
    TRACE("%s\n", #Name);                                                                                               \
    HandleScope scope;                                                                                                  \
                                                                                                                        \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());                                                      \
    assert(obj != NULL);                                                                                                \
                                                                                                                        \
    if ( (args.Length() == 0) ||                                                                                        \
         (args.Length() == 1 && (!args[0]->IsBoolean()) & !args[0]->IsFunction()) ) {                                   \
      ThrowException(Exception::TypeError(String::New("Bad argument")));                                                \
      return args.This();                                                                                               \
    }                                                                                                                   \
                                                                                                                        \ 
    PolyDB::Error::Code result = PolyDB::Error::SUCCESS;                                                                \
    Local<Function> cb;                                                                                                 \
    bool flag = INIT_VALUE;                                                                                             \
    cb.Clear();                                                                                                         \
                                                                                                                        \ 
    if (args.Length() == 1) {                                                                                           \
      if (args[0]->IsFunction()) {                                                                                      \
        cb = Local<Function>::New(Handle<Function>::Cast(args[0]));                                                     \
      } else if (args[0]->IsBoolean()) {                                                                                \
        flag = args[0]->BooleanValue();                                                                                 \
      }                                                                                                                 \
    } else {                                                                                                            \
      flag = args[0]->BooleanValue();                                                                                   \
      cb = Local<Function>::New(Handle<Function>::Cast(args[1]));                                                       \
    }                                                                                                                   \
                                                                                                                        \
    TRACE("call %s: flag = %d\n", #Method, flag);                                                                       \
    if (!obj->db_->Method(flag)) {                                                                                      \
      result = obj->db_->error().code();                                                                                \
    }                                                                                                                   \
                                                                                                                        \ 
    Local<Value> argv[1] = {                                                                                            \
      Local<Value>::New(Null()),                                                                                        \
    };                                                                                                                  \
                                                                                                                        \ 
    if (result != PolyDB::Error::SUCCESS) {                                                                             \
      const char *name = PolyDB::Error::codename(result);                                                               \
      Local<String> message = String::NewSymbol(name);                                                                  \
      Local<Value> err = Exception::Error(message);                                                                     \
      Local<Object> obj = err->ToObject();                                                                              \
      obj->Set(String::NewSymbol("code"), Integer::New(result), static_cast<PropertyAttribute>(ReadOnly | DontDelete)); \
      argv[0] = err;                                                                                                    \
    }                                                                                                                   \
                                                                                                                        \ 
    if (!cb.IsEmpty()) {                                                                                                \
      TryCatch try_catch;                                                                                               \
      MakeCallback(obj->handle_, cb, 1, argv);                                                                          \
      if (try_catch.HasCaught()) {                                                                                      \
        FatalException(try_catch);                                                                                      \
      }                                                                                                                 \
    }                                                                                                                   \ 
                                                                                                                        \ 
    return scope.Close(args.This());                                                                                    \
  }                                                                                                                     \

#define DEFINE_RET_FUNC(Name, Type, REQ_TYPE, INIT_VALUE)                   \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                   \
    HandleScope scope;                                                      \
                                                                            \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());          \
    assert(obj != NULL);                                                    \
                                                                            \
    if ((args.Length() == 1 && (!args[0]->IsObject())                       \
                              | !args[0]->IsFunction())) {                  \
      ThrowException(Exception::TypeError(String::New("Bad argument")));    \
      return args.This();                                                   \
    }                                                                       \
                                                                            \
    REQ_TYPE *req = (REQ_TYPE *)malloc(sizeof(REQ_TYPE));                   \
    req->type = Type;                                                       \
    req->result = PolyDB::Error::SUCCESS;                                   \
    req->wrapdb = obj;                                                      \
    req->cb.Clear();                                                        \
    req->ret = INIT_VALUE;                                                  \
                                                                            \
    if (args.Length() > 0 && args[0]->IsFunction()) {                       \
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0])); \
    }                                                                       \
                                                                            \ 
    uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));             \
    uv_req->data = req;                                                     \
                                                                            \
    int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone); \
    TRACE("uv_queue_work: ret=%d\n", ret);                                  \
                                                                            \
    obj->Ref();                                                             \
    return args.This();                                                     \
  }                                                                         \

#define DEFINE_KV_K_ONLY(Name, Type)                                            \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                       \
    HandleScope scope;                                                          \
                                                                                \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());              \
    assert(obj != NULL);                                                        \
                                                                                \
    Local<String> key_sym = String::NewSymbol("key");                           \
    if ((args.Length() == 0)                                                    \
        || (args.Length() == 1                                                  \
          && (!args[0]->IsObject()) | !args[0]->IsFunction())                   \
        || (args.Length() == 1                                                  \
          && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym)           \
          && !args[0]->ToObject()->Get(key_sym)->IsString())                    \
        || (args.Length() == 2                                                  \
          && (!args[0]->IsObject() || !args[1]->IsFunction()))                  \
        || (args.Length() == 2                                                  \
          && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym)           \
          && !args[0]->ToObject()->Get(key_sym)->IsString())) {                 \
      ThrowException(Exception::TypeError(String::New("Bad argument")));        \
      return args.This();                                                       \
    }                                                                           \
                                                                                \
    kc_kv_req_t *req = (kc_kv_req_t *)malloc(sizeof(kc_kv_req_t));              \
    req->type = Type;                                                           \
    req->result = PolyDB::Error::SUCCESS;                                       \
    req->wrapdb = obj;                                                          \
    req->cb.Clear();                                                            \
    req->key = NULL;                                                            \
    req->value = NULL;                                                          \
                                                                                \
    if (args.Length() == 1) {                                                   \
      if (args[0]->IsFunction()) {                                              \
        req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));   \
      } else if (args[0]->IsObject()) {                                         \
        assert(args[0]->IsObject());                                            \
        if (args[0]->ToObject()->Has(key_sym)) {                                \
          String::Utf8Value key(args[0]->ToObject()->Get(key_sym));             \
          req->key = kc::strdup(*key);                                          \
        }                                                                       \
      }                                                                         \
    } else {                                                                    \
      if (args[0]->ToObject()->Has(key_sym)) {                                  \
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));               \
        req->key = kc::strdup(*key);                                            \
      }                                                                         \
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));     \
    }                                                                           \
                                                                                \ 
    uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));                 \
    uv_req->data = req;                                                         \
                                                                                \
    int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);     \
    TRACE("uv_queue_work: ret=%d\n", ret);                                      \
                                                                                \
    obj->Ref();                                                                 \
    return args.This();                                                         \
  }                                                                             \

#define DEFINE_KV_FUNC(Name, Type)                                              \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                       \
    HandleScope scope;                                                          \
                                                                                \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());              \
    assert(obj != NULL);                                                        \
                                                                                \
    Local<String> key_sym = String::NewSymbol("key");                           \
    Local<String> value_sym = String::NewSymbol("value");                       \
    if ((args.Length() == 0)                                                    \
        || (args.Length() == 1                                                  \
          && (!args[0]->IsObject()) | !args[0]->IsFunction())                   \
        || (args.Length() == 1                                                  \
          && args[0]->IsObject()                                                \ 
          && args[0]->ToObject()->Has(key_sym)                                  \ 
          && !args[0]->ToObject()->Get(key_sym)->IsString())                    \
        || (args.Length() == 1                                                  \ 
          && args[0]->IsObject()                                                \ 
          && args[0]->ToObject()->Has(value_sym)                                \ 
          && !args[0]->ToObject()->Get(value_sym)->IsString())                  \
        || (args.Length() == 2                                                  \ 
          && (!args[0]->IsObject() || !args[1]->IsFunction()))                  \
        || (args.Length() == 2                                                  \ 
          && args[0]->IsObject()                                                \ 
          && args[0]->ToObject()->Has(key_sym)                                  \ 
          && !args[0]->ToObject()->Get(key_sym)->IsString())                    \
        || (args.Length() == 2                                                  \
          && args[0]->IsObject()                                                \ 
          && args[0]->ToObject()->Has(value_sym)                                \ 
          && !args[0]->ToObject()->Get(value_sym)->IsString())) {               \
      ThrowException(Exception::TypeError(String::New("Bad argument")));        \
      return args.This();                                                       \
    }                                                                           \
                                                                                \
    kc_kv_req_t *req = (kc_kv_req_t *)malloc(sizeof(kc_kv_req_t));              \
    req->type = Type;                                                           \
    req->result = PolyDB::Error::SUCCESS;                                       \
    req->wrapdb = obj;                                                          \
    req->cb.Clear();                                                            \
    req->key = NULL;                                                            \
    req->value = NULL;                                                          \
                                                                                \
    if (args.Length() == 1) {                                                   \
      if (args[0]->IsFunction()) {                                              \
        req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));   \
      } else {                                                                  \
        assert(args[0]->IsObject());                                            \
        if (args[0]->ToObject()->Has(key_sym)) {                                \
          String::Utf8Value key(args[0]->ToObject()->Get(key_sym));             \
          req->key = kc::strdup(*key);                                          \
        }                                                                       \
        if (args[0]->ToObject()->Has(value_sym)) {                              \
          String::Utf8Value value(args[0]->ToObject()->Get(value_sym));         \
          req->value = kc::strdup(*value);                                      \
        }                                                                       \
      }                                                                         \
    } else {                                                                    \
      assert(args[0]->IsObject() && args[1]->IsFunction());                     \
      if (args[0]->ToObject()->Has(key_sym)) {                                  \
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));               \
        req->key = kc::strdup(*key);                                            \
      }                                                                         \
      if (args[0]->ToObject()->Has(value_sym)) {                                \
        String::Utf8Value value(args[0]->ToObject()->Get(value_sym));           \
        req->value = kc::strdup(*value);                                        \
      }                                                                         \
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));     \
    }                                                                           \
                                                                                \ 
    uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));                 \
    uv_req->data = req;                                                         \
                                                                                \
    int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);     \
    TRACE("uv_queue_work: ret=%d\n", ret);                                      \
                                                                                \
    obj->Ref();                                                                 \
    return args.This();                                                         \
  }                                                                             \

#define DO_EXECUTE(Method)              \
  if (!db->Method()) {                  \
    req->result = db->error().code();   \
  }                                     \

#define DO_CHAR_PARAM_CMN_EXECUTE(Method, WorkReq)                         \
  kc_char_cmn_req_t *req = static_cast<kc_char_cmn_req_t*>(WorkReq->data); \
  if (req->str == NULL) {                                                  \
    req->result = PolyDB::Error::INVALID;                                  \
  } else {                                                                 \
    if (!db->Method(req->str)) {                                           \
      req->result = db->error().code();                                    \
    }                                                                      \
  }                                                                        \

#define DO_BOOL_PARAM_CMN_EXECUTE(Method, WorkReq)                                \
  kc_boolean_cmn_req_t *req = static_cast<kc_boolean_cmn_req_t*>(WorkReq->data);  \
  if (!db->Method(req->flag)) {                                                   \
    req->result = db->error().code();                                             \
  }                                                                               \

#define DO_RET_EXECUTE(Method, WorkReq)                             \
  kc_ret_req_t *req = static_cast<kc_ret_req_t*>(WorkReq->data);    \
  req->ret = db->Method();                                          \
  TRACE("ret = %d\n", req->ret);                                    \
  if (req->ret == -1) {                                             \
    req->result = db->error().code();                               \
  }                                                                 \

#define DO_KV_V_RET_EXECUTE(Method, WorkReq)                            \
  kc_kv_req_t *req = static_cast<kc_kv_req_t*>(WorkReq->data);          \
  TRACE("key = %s\n", req->key);                                        \
  size_t value_size;                                                    \
  if (req->key == NULL) {                                               \
    req->result = PolyDB::Error::INVALID;                               \
  } else {                                                              \
    req->value = db->Method(req->key, strlen(req->key), &value_size);   \
    TRACE("return value = %s, size = %d\n", req->value, value_size);    \
    if (req->value == NULL) {                                           \
      req->result = db->error().code();                                 \
    }                                                                   \
  }                                                                     \

#define DO_KV_EXECUTE(Method, WorkReq)                            \
  kc_kv_req_t *req = static_cast<kc_kv_req_t*>(WorkReq->data);    \
  TRACE("key = %s, value = %s\n", req->key, req->value);          \
  if (req->key == NULL || req->value == NULL) {                   \
    req->result = PolyDB::Error::INVALID;                         \
  } else {                                                        \
    if (!db->Method(req->key, strlen(req->key),                   \
                    req->value, strlen(req->value))) {            \
      req->result = db->error().code();                           \
    }                                                             \
  }                                                               \

#define DO_MATCH_CMN_EXECUTE(Method, WorkReq)                                               \
  kc_match_cmn_req_t *req = static_cast<kc_match_cmn_req_t*>(WorkReq->data);                \
  if (req->str == NULL) {                                                                   \
    req->result = PolyDB::Error::INVALID;                                                   \
  } else {                                                                                  \
    req->keys = new StringVector();                                                         \
    int64_t num = db->Method(std::string(req->str, strlen(req->str)), req->keys, req->max); \
    TRACE("match return: %ld\n", num);                                                      \
    if (num <= 0) {                                                                         \
      req->result = db->error().code();                                                     \
    }                                                                                       \
  }                                                                                         \

#define SAFE_REQ_ATTR_FREE(Req, AttrName)   \
  if (Req->AttrName != NULL) {              \
    free(Req->AttrName);                    \
    Req->AttrName = NULL;                   \
  }                                         \

#define SAFE_REQ_ATTR_DELETE(Req, AttrName)   \
  if (Req->AttrName) {                        \
    delete Req->AttrName;                     \
    Req->AttrName = NULL;                     \
  }                                           \


// request type
enum kc_req_type {
  KC_OPEN,
  KC_CLOSE,
  KC_SET,
  KC_GET,
  KC_CLEAR,
  KC_ADD,
  KC_APPEND,
  KC_REMOVE,
  KC_REPLACE,
  KC_SEIZE,
  KC_INCREMENT,
  KC_INCREMENT_DOUBLE,
  KC_CAS,
  KC_COUNT,
  KC_SIZE,
  KC_STATUS,
  KC_CHECK,
  KC_GET_BULK,
  KC_SET_BULK,
  KC_REMOVE_BULK,
  KC_MATCH_PREFIX,
  KC_MATCH_REGEX,
  KC_MATCH_SIMILAR,
  KC_COPY,
  KC_MERGE,
  KC_DUMP_SNAPSHOT,
  KC_LOAD_SNAPSHOT,
  KC_ACCEPT,
  KC_ACCEPT_BULK,
  KC_BEGIN_TRANSACTION,
  KC_END_TRANSACTION,
};

// common request field
#define KC_REQ_FIELD            \
  PolyDBWrap *wrapdb;           \
  kc_req_type type;             \
  PolyDB::Error::Code result;   \
  Persistent<Function> cb;      \

typedef std::map<std::string, std::string> StringMap;
typedef std::vector<std::string> StringVector;

// base request
typedef struct kc_req_t {
  KC_REQ_FIELD
} kc_req_t;

// open request
typedef struct kc_open_req_t {
  KC_REQ_FIELD
  char *path;
  uint32_t mode;
} kc_open_req_t;

// key/value params common request
typedef struct kc_kv_req_t {
  KC_REQ_FIELD
  char *key;
  char *value;
} kc_kv_req_t;

// increment request
typedef struct kc_increment_req_t {
  KC_REQ_FIELD
  char *key;
  int64_t num;
  int64_t orig;
} kc_increment_req_t;

// increment_double request
typedef struct kc_increment_double_req_t {
  KC_REQ_FIELD
  char *key;
  double num;
  double orig;
} kc_increment_double_req_t;

// cas request
typedef struct kc_cas_req_t {
  KC_REQ_FIELD
  char *key;
  char *oval;
  char *nval;
} kc_cas_req_t;

// return value common request
typedef struct kc_ret_req_t {
  KC_REQ_FIELD
  int64_t ret;
} kc_ret_req_t;

// string map value request
typedef struct kc_strmap_ret_req_t {
  KC_REQ_FIELD
  StringMap *ret;
} kc_strmap_ret_req_t;

// check request
typedef struct kc_check_req_t {
  KC_REQ_FIELD
  char *key;
  int64_t ret;
} kc_check_req_t;

// get_bulk request
typedef struct kc_get_bulk_req_t {
  KC_REQ_FIELD
  StringVector *keys;
  bool atomic;
  StringMap *recs;
} kc_get_bulk_req_t;

// set_bulk request
typedef struct kc_set_bulk_req_t {
  KC_REQ_FIELD
  StringMap *recs;
  bool atomic;
  int64_t num;
} kc_set_bulk_req_t;

// remove_bulk request
typedef struct kc_remove_bulk_req_t {
  KC_REQ_FIELD
  StringVector *keys;
  bool atomic;
  int64_t num;
} kc_remove_bulk_req_t;

// match common request field
#define KC_MATCH_REQ_FIELD    \
  char *str;                  \
  int64_t max;                \
  StringVector *keys;         \

// match common request
typedef struct kc_match_cmn_req_t {
  KC_REQ_FIELD
  KC_MATCH_REQ_FIELD
} kc_match_cmn_req_t;

// match_similar request
typedef struct kc_match_similar_req_t {
  KC_REQ_FIELD
  KC_MATCH_REQ_FIELD
  int64_t range;
  bool utf;
} kc_match_similar_req_t;

// charactor pointer type parameter common request
typedef struct kc_char_cmn_req_t {
  KC_REQ_FIELD
  char *str;
} kc_char_cmn_req_t;

// merge request
typedef struct kc_merge_req_t {
  KC_REQ_FIELD
  kc::BasicDB **srcary;
  int32_t srcnum;
  uint32_t mode;
} kc_merge_req_t;

// accept request
typedef struct kc_accept_req_t {
  KC_REQ_FIELD
  char *key;
  Persistent<Object> visitor;
  bool writable;
} kc_accept_req_t;

// boolean parameter common request
typedef struct kc_boolean_cmn_req_t {
  KC_REQ_FIELD
  bool flag;
} kc_boolean_cmn_req_t;



class InternalVisitor : public PolyDB::Visitor {
public:
  Local<Object> &visitor_;
  bool writable_;

  explicit InternalVisitor(Local<Object> &visitor, bool writable) :
    visitor_(visitor), writable_(writable) {
    TRACE("ctor\n");
  }
  ~InternalVisitor() {
    TRACE("destor\n");
  }
private:
  const char* visit_full(const char *kbuf, size_t ksiz,
                         const char *vbuf, size_t vsiz, size_t *sp) {
    HandleScope scope;
    TRACE("arguments: kbuf = %s, ksiz = %d, vbuf = %s, vsiz = %d, sp = %d\n", kbuf, ksiz, vbuf, vsiz, *sp);

    const char *rv = NOP;
    Local<Value> ret;
    Local<String> method_name = String::NewSymbol("visit_full");
    Local<Function> cb;
    if (!visitor_->IsFunction()) {
      if (!visitor_->ToObject()->Has(method_name) 
          && !visitor_->ToObject()->Get(method_name)->IsFunction()) {
        rv = NOP;
      }
      cb = Local<Function>::New(Handle<Function>::Cast(visitor_->ToObject()->Get(method_name)));
    } else {
      cb = Local<Function>::New(Handle<Function>::Cast(visitor_));
    }

    if (!cb.IsEmpty()) {
      Local<Value> argv[2] = { 
        String::New(kbuf, ksiz),
        String::New(vbuf, vsiz),
      };
      TryCatch try_catch;
      ret = cb->Call(Context::GetCurrent()->Global(), 2, argv);
      if (try_catch.HasCaught()) {
        FatalException(try_catch);
      }

      if (writable_) {
        if (!ret.IsEmpty()) {
          if (ret->IsNumber()) {
            int64_t num_ret = ret->IntegerValue();
            if (num_ret == 1) {
              rv = REMOVE;
            }
          } else if (ret->IsString()) {
            String::Utf8Value str_ret(ret->ToString());
            if (*str_ret) {
              //rv = *str_ret;
              //*sp = strlen(*str_ret);
              rv = kc::strdup(*str_ret);
              *sp = strlen(rv);
            }
          }
        }
      } else {
        rv = NULL;
      }
    }
    
    return rv;
  }
  const char* visit_empty(const char *kbuf, size_t ksiz, size_t *sp) {
    HandleScope scope;
    TRACE("arguments: kbuf = %s, ksiz = %d, sp = %d\n", kbuf, ksiz, *sp);

    const char *rv = NOP;
    Local<Value> ret;
    Local<String> method_name = String::NewSymbol("visit_empty");
    Local<Function> cb;
    if (!visitor_->IsFunction()) {
      if (!visitor_->ToObject()->Has(method_name) 
          && !visitor_->ToObject()->Get(method_name)->IsFunction()) {
        rv = NOP;
      }
      cb = Local<Function>::New(Handle<Function>::Cast(visitor_->ToObject()->Get(method_name)));
    } else {
      cb = Local<Function>::New(Handle<Function>::Cast(visitor_));
    }

    if (!cb.IsEmpty()) {
      Local<Value> argv[1] = { 
        String::New(kbuf, ksiz),
      };
      TryCatch try_catch;
      ret = cb->Call(Context::GetCurrent()->Global(), 1, argv);
      if (try_catch.HasCaught()) {
        FatalException(try_catch);
      }

      if (writable_) {
        if (!ret.IsEmpty()) {
          if (ret->IsNumber()) {
            int64_t num_ret = ret->IntegerValue();
            if (num_ret == 1) {
              rv = REMOVE;
            }
          } else if (ret->IsString()) {
            String::Utf8Value str_ret(ret->ToString());
            if (*str_ret) {
              //rv = *str_ret;
              //*sp = strlen(*str_ret);
              rv = kc::strdup(*str_ret);
              *sp = strlen(rv);
            }
          }
        }
      } else {
        rv = NULL;
      }
    }

    return rv;
  }
  /*
  void visit_before() {
    TRACE("call visit_before\n");
  }
  void visit_after() {
    TRACE("call visit_after\n");
  }
  */
};


StringVector* Array2Vector(const Local<Value> obj) {
  StringVector *vector = new StringVector();
  Local<Array> array = Local<Array>::Cast(obj);
  int len = array->Length();
  for (int i = 0; i < len; i++) {
    String::Utf8Value val(array->Get(Integer::New(i))->ToString());
    vector->push_back(std::string(*val, val.length()));
  }
  return vector;
}

Local<Array> Vector2Array(const StringVector *vector) {
  StringVector::const_iterator it = vector->begin();
  StringVector::const_iterator it_end = vector->end();
  int32_t index = 0;
  Local<Array> array = Array::New(vector->size());
  while (it != it_end) {
    Local<String> val = String::New(it->c_str(), it->length());
    array->Set(index++, val);
    ++it;
  }
  return array;
}

Local<Object> Map2Obj(const StringMap *map) {
  Local<Object> obj = Object::New();
  StringMap::const_iterator it = map->begin();
  StringMap::const_iterator it_end = map->end();
  while (it != it_end) {
    Local<String> key = String::New(it->first.c_str(), it->first.length());
    Local<String> val = String::New(it->second.c_str(), it->second.length());
    obj->Set(key, val);
    ++it;
  }
  return obj;
}

StringMap* Obj2Map(const Local<Value> target) {
  StringMap *result = new StringMap();
  Local<Object> obj = Local<Object>::Cast(target);
  Local<Array> names = obj->GetPropertyNames();
  int len = names->Length();
  for (int i = 0; i < len; i++) {
    Local<Value> name = names->Get(Integer::New(i));
    String::Utf8Value key(name);
    String::Utf8Value val(obj->Get(name)->ToString());
    std::string std_key = std::string(*key, key.length());
    std::string std_val = std::string(*val, val.length());
    result->insert(std::pair<std::string, std::string>(std_key, std_val));
  }
  return result;
}


PolyDBWrap::PolyDBWrap() {
  db_ = new PolyDB();
  TRACE("ctor: db_ = %p\n", db_);
  assert(db_ != NULL);
}

PolyDBWrap::~PolyDBWrap() {
  assert(db_ != NULL);
  TRACE("destor: db_ = %p\n", db_);
  delete db_;
  db_ = NULL;
}

Handle<Value> PolyDBWrap::New(const Arguments &args) {
  TRACE("New\n");
  HandleScope scope;

  PolyDBWrap *obj = new PolyDBWrap();
  obj->Wrap(args.This());

  return args.This();
}

Handle<Value> PolyDBWrap::Open(const Arguments &args) {
  TRACE("Open\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  kc_open_req_t *open_req = (kc_open_req_t *)malloc(sizeof(kc_open_req_t));
  open_req->type = KC_OPEN;
  open_req->result = PolyDB::Error::SUCCESS;
  open_req->wrapdb = obj;
  open_req->cb.Clear();

  if (args.Length() == 0) {
    String::Utf8Value str(String::NewSymbol(":")->ToString());
    open_req->path = kc::strdup(*str);
    open_req->mode = 0;
  } else if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      String::Utf8Value str(String::NewSymbol(":")->ToString());
      open_req->path = kc::strdup(*str);
      open_req->mode = 0;
      open_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      String::Utf8Value str(args[0]->ToObject()->Get(String::NewSymbol("path"))->ToString());
      open_req->path = kc::strdup(*str);
      open_req->mode = args[0]->ToObject()->Get(String::NewSymbol("mode"))->Uint32Value();
      open_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
    }
  } else {
    String::Utf8Value str(args[0]->ToObject()->Get(String::NewSymbol("path"))->ToString());
    open_req->path = kc::strdup(*str);
    open_req->mode = args[0]->ToObject()->Get(String::NewSymbol("mode"))->Uint32Value();
    open_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = open_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

DEFINE_FUNC(Close, KC_CLOSE);
DEFINE_KV_FUNC(Set, KC_SET);
DEFINE_KV_K_ONLY(Get, KC_GET);
DEFINE_FUNC(Clear, KC_CLEAR);
DEFINE_KV_FUNC(Add, KC_ADD);
DEFINE_KV_FUNC(Append, KC_APPEND);

Handle<Value> PolyDBWrap::Remove(const Arguments &args) {
  TRACE("Remove\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_char_cmn_req_t *req = (kc_char_cmn_req_t *)malloc(sizeof(kc_char_cmn_req_t));
  req->type = KC_REMOVE;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->str = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        req->str = kc::strdup(*key);
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      req->str = kc::strdup(*key);
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

DEFINE_KV_FUNC(Replace, KC_REPLACE);
DEFINE_KV_K_ONLY(Seize, KC_SEIZE);

Handle<Value> PolyDBWrap::Increment(const Arguments &args) {
  TRACE("Increment\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  Local<String> num_sym = String::NewSymbol("num");
  Local<String> orig_sym = String::NewSymbol("orig");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(num_sym) && !args[0]->ToObject()->Get(num_sym)->IsNumber()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(orig_sym) && !args[0]->ToObject()->Get(orig_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(num_sym) && !args[0]->ToObject()->Get(num_sym)->IsNumber()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(orig_sym) && !args[0]->ToObject()->Get(orig_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_increment_req_t *increment_req = (kc_increment_req_t *)malloc(sizeof(kc_increment_req_t));
  increment_req->type = KC_INCREMENT;
  increment_req->result = PolyDB::Error::SUCCESS;
  increment_req->wrapdb = obj;
  increment_req->cb.Clear();
  increment_req->key = NULL;
  increment_req->num = 0;
  increment_req->orig = 0;

  double orig = 0;
  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      increment_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        increment_req->key = kc::strdup(*key);
      }
      if (args[0]->ToObject()->Has(num_sym)) {
        increment_req->num = args[0]->ToObject()->Get(num_sym)->IntegerValue();
      }
      if (args[0]->ToObject()->Has(orig_sym)) {
        orig = args[0]->ToObject()->Get(orig_sym)->NumberValue();
        if (kc::chkinf(orig)) {
          increment_req->orig = (orig >= 0 ? kc::INT64MAX : kc::INT64MIN);
        } else {
          increment_req->orig = orig;
        }
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      increment_req->key = kc::strdup(*key);
    }
    if (args[0]->ToObject()->Has(num_sym)) {
      increment_req->num = args[0]->ToObject()->Get(num_sym)->IntegerValue();
    }
    if (args[0]->ToObject()->Has(orig_sym)) {
      orig = args[0]->ToObject()->Get(orig_sym)->NumberValue();
      //TRACE("argument orig = %f\n", orig);
      if (kc::chkinf(orig)) {
        increment_req->orig = (orig >= 0 ? kc::INT64MAX : kc::INT64MIN);
      } else {
        increment_req->orig = orig;
      }
    }
    increment_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = increment_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::IncrementDouble(const Arguments &args) {
  TRACE("IncrementDouble\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  Local<String> num_sym = String::NewSymbol("num");
  Local<String> orig_sym = String::NewSymbol("orig");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(num_sym) && !args[0]->ToObject()->Get(num_sym)->IsNumber()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(orig_sym) && !args[0]->ToObject()->Get(orig_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(num_sym) && !args[0]->ToObject()->Get(num_sym)->IsNumber()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(orig_sym) && !args[0]->ToObject()->Get(orig_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_increment_double_req_t *inc_dbl_req = (kc_increment_double_req_t *)malloc(sizeof(kc_increment_double_req_t));
  inc_dbl_req->type = KC_INCREMENT_DOUBLE;
  inc_dbl_req->result = PolyDB::Error::SUCCESS;
  inc_dbl_req->wrapdb = obj;
  inc_dbl_req->cb.Clear();
  inc_dbl_req->key = NULL;
  inc_dbl_req->num = 0;
  inc_dbl_req->orig = 0;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      inc_dbl_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        inc_dbl_req->key = kc::strdup(*key);
      }
      if (args[0]->ToObject()->Has(num_sym)) {
        inc_dbl_req->num = args[0]->ToObject()->Get(num_sym)->NumberValue();
      }
      if (args[0]->ToObject()->Has(orig_sym)) {
        inc_dbl_req->orig = args[0]->ToObject()->Get(orig_sym)->NumberValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      inc_dbl_req->key = kc::strdup(*key);
    }
    if (args[0]->ToObject()->Has(num_sym)) {
      inc_dbl_req->num = args[0]->ToObject()->Get(num_sym)->NumberValue();
    }
    if (args[0]->ToObject()->Has(orig_sym)) {
      inc_dbl_req->orig = args[0]->ToObject()->Get(orig_sym)->NumberValue();
    }
    inc_dbl_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = inc_dbl_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::Cas(const Arguments &args) {
  TRACE("Cas\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  Local<String> oval_sym = String::NewSymbol("oval");
  Local<String> nval_sym = String::NewSymbol("nval");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(oval_sym) && !args[0]->ToObject()->Get(oval_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(nval_sym) && !args[0]->ToObject()->Get(nval_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(oval_sym) && !args[0]->ToObject()->Get(oval_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(nval_sym) && !args[0]->ToObject()->Get(nval_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_cas_req_t *cas_req = (kc_cas_req_t *)malloc(sizeof(kc_cas_req_t));
  cas_req->type = KC_CAS;
  cas_req->result = PolyDB::Error::SUCCESS;
  cas_req->wrapdb = obj;
  cas_req->cb.Clear();
  cas_req->key = NULL;
  cas_req->oval = NULL;
  cas_req->nval = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      cas_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        cas_req->key = kc::strdup(*key);
      }
      if (args[0]->ToObject()->Has(oval_sym)) {
        String::Utf8Value oval(args[0]->ToObject()->Get(oval_sym));
        cas_req->oval = kc::strdup(*oval);
      }
      if (args[0]->ToObject()->Has(nval_sym)) {
        String::Utf8Value nval(args[0]->ToObject()->Get(nval_sym));
        cas_req->nval = kc::strdup(*nval);
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      cas_req->key = kc::strdup(*key);
    }
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      cas_req->key = kc::strdup(*key);
    }
    if (args[0]->ToObject()->Has(oval_sym)) {
      String::Utf8Value oval(args[0]->ToObject()->Get(oval_sym));
      cas_req->oval = kc::strdup(*oval);
    }
    if (args[0]->ToObject()->Has(nval_sym)) {
      String::Utf8Value nval(args[0]->ToObject()->Get(nval_sym));
      cas_req->nval = kc::strdup(*nval);
    }
    cas_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = cas_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

DEFINE_RET_FUNC(Count, KC_COUNT, kc_ret_req_t, -1);
DEFINE_RET_FUNC(Size, KC_SIZE, kc_ret_req_t, -1);
DEFINE_RET_FUNC(Status, KC_STATUS, kc_strmap_ret_req_t, NULL);

Handle<Value> PolyDBWrap::Check(const Arguments &args) {
  TRACE("Check\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_check_req_t *check_req = (kc_check_req_t *)malloc(sizeof(kc_check_req_t));
  check_req->type = KC_CHECK;
  check_req->result = PolyDB::Error::SUCCESS;
  check_req->wrapdb = obj;
  check_req->cb.Clear();
  check_req->key = NULL;
  check_req->ret = -1;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      check_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        check_req->key = kc::strdup(*key);
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      check_req->key = kc::strdup(*key);
    }
    check_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = check_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::GetBulk(const Arguments &args) {
  TRACE("GetBulk\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> keys_sym = String::NewSymbol("keys");
  Local<String> atomic_sym = String::NewSymbol("atomic");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && !args[0]->ToObject()->Get(keys_sym)->IsArray()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && !args[0]->ToObject()->Get(keys_sym)->IsArray()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_get_bulk_req_t *req = (kc_get_bulk_req_t *)malloc(sizeof(kc_get_bulk_req_t));
  req->type = KC_GET_BULK;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->keys = NULL;
  req->atomic = true;
  req->recs = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(keys_sym)) {
        req->keys = Array2Vector(args[0]->ToObject()->Get(keys_sym));
      }
      if (args[0]->ToObject()->Has(atomic_sym)) {
        req->atomic = args[0]->ToObject()->Get(atomic_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(keys_sym)) {
      req->keys = Array2Vector(args[0]->ToObject()->Get(keys_sym));
    }
    if (args[0]->ToObject()->Has(atomic_sym)) {
      req->atomic = args[0]->ToObject()->Get(atomic_sym)->BooleanValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::SetBulk(const Arguments &args) {
  TRACE("SetBulk\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> recs_sym = String::NewSymbol("recs");
  Local<String> atomic_sym = String::NewSymbol("atomic");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(recs_sym) && !args[0]->ToObject()->Get(recs_sym)->IsObject()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(recs_sym) && !args[0]->ToObject()->Get(recs_sym)->IsObject()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_set_bulk_req_t *req = (kc_set_bulk_req_t *)malloc(sizeof(kc_set_bulk_req_t));
  req->type = KC_SET_BULK;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->recs = NULL;
  req->atomic = true;
  req->num = -1;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(recs_sym)) {
        req->recs = Obj2Map(args[0]->ToObject()->Get(recs_sym));
      }
      if (args[0]->ToObject()->Has(atomic_sym)) {
        req->atomic = args[0]->ToObject()->Get(atomic_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(recs_sym)) {
      req->recs = Obj2Map(args[0]->ToObject()->Get(recs_sym));
    }
    if (args[0]->ToObject()->Has(atomic_sym)) {
      req->atomic = args[0]->ToObject()->Get(atomic_sym)->BooleanValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::RemoveBulk(const Arguments &args) {
  TRACE("RemoveBulk\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> keys_sym = String::NewSymbol("keys");
  Local<String> atomic_sym = String::NewSymbol("atomic");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && !args[0]->ToObject()->Get(keys_sym)->IsArray()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && !args[0]->ToObject()->Get(keys_sym)->IsArray()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_remove_bulk_req_t *req = (kc_remove_bulk_req_t *)malloc(sizeof(kc_remove_bulk_req_t));
  req->type = KC_REMOVE_BULK;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->keys = NULL;
  req->atomic = true;
  req->num = -1;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(keys_sym)) {
        req->keys = Array2Vector(args[0]->ToObject()->Get(keys_sym));
      }
      if (args[0]->ToObject()->Has(atomic_sym)) {
        req->atomic = args[0]->ToObject()->Get(atomic_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(keys_sym)) {
      req->keys = Array2Vector(args[0]->ToObject()->Get(keys_sym));
    }
    if (args[0]->ToObject()->Has(atomic_sym)) {
      req->atomic = args[0]->ToObject()->Get(atomic_sym)->BooleanValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::MatchPrefix(const Arguments &args) {
  TRACE("MatchPrefix\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> prefix_sym = String::NewSymbol("prefix");
  Local<String> max_sym = String::NewSymbol("max");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(prefix_sym) && !args[0]->ToObject()->Get(prefix_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && !args[0]->ToObject()->Get(max_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(prefix_sym) && !args[0]->ToObject()->Get(prefix_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && !args[0]->ToObject()->Get(max_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_match_cmn_req_t *req = (kc_match_cmn_req_t *)malloc(sizeof(kc_match_cmn_req_t));
  req->type = KC_MATCH_PREFIX;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->str = NULL;
  req->max = -1;
  req->keys = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(prefix_sym)) {
        String::Utf8Value prefix(args[0]->ToObject()->Get(prefix_sym));
        req->str = kc::strdup(*prefix);
      }
      if (args[0]->ToObject()->Has(max_sym)) {
        req->max = args[0]->ToObject()->Get(max_sym)->NumberValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(prefix_sym)) {
      String::Utf8Value prefix(args[0]->ToObject()->Get(prefix_sym));
      req->str = kc::strdup(*prefix);
    }
    if (args[0]->ToObject()->Has(max_sym)) {
      req->max = args[0]->ToObject()->Get(max_sym)->NumberValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::MatchRegex(const Arguments &args) {
  TRACE("MatchRegex\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> regex_sym = String::NewSymbol("regex");
  Local<String> max_sym = String::NewSymbol("max");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(regex_sym) && !args[0]->ToObject()->Get(regex_sym)->IsRegExp()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && !args[0]->ToObject()->Get(max_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(regex_sym) && !args[0]->ToObject()->Get(regex_sym)->IsRegExp()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && !args[0]->ToObject()->Get(max_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_match_cmn_req_t *req = (kc_match_cmn_req_t *)malloc(sizeof(kc_match_cmn_req_t));
  req->type = KC_MATCH_REGEX;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->str = NULL;
  req->max = -1;
  req->keys = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(regex_sym)) {
        String::Utf8Value regex(Local<RegExp>::Cast(args[0]->ToObject()->Get(regex_sym))->GetSource());
        req->str = kc::strdup(*regex);
      }
      if (args[0]->ToObject()->Has(max_sym)) {
        req->max = args[0]->ToObject()->Get(max_sym)->NumberValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(regex_sym)) {
      String::Utf8Value regex(Local<RegExp>::Cast(args[0]->ToObject()->Get(regex_sym))->GetSource());
      req->str = kc::strdup(*regex);
    }
    if (args[0]->ToObject()->Has(max_sym)) {
      req->max = args[0]->ToObject()->Get(max_sym)->NumberValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::MatchSimilar(const Arguments &args) {
  TRACE("MatchSimilar\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> origin_sym = String::NewSymbol("origin");
  Local<String> max_sym = String::NewSymbol("max");
  Local<String> range_sym = String::NewSymbol("range");
  Local<String> utf_sym = String::NewSymbol("utf");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(origin_sym) && !args[0]->ToObject()->Get(origin_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && !args[0]->ToObject()->Get(max_sym)->IsNumber()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(range_sym) && !args[0]->ToObject()->Get(range_sym)->IsNumber()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(utf_sym) && !args[0]->ToObject()->Get(utf_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(origin_sym) && !args[0]->ToObject()->Get(origin_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && !args[0]->ToObject()->Get(max_sym)->IsNumber()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(range_sym) && !args[0]->ToObject()->Get(range_sym)->IsNumber()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(utf_sym) && !args[0]->ToObject()->Get(utf_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_match_similar_req_t *req = (kc_match_similar_req_t *)malloc(sizeof(kc_match_similar_req_t));
  req->type = KC_MATCH_SIMILAR;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->str = NULL;
  req->max = -1;
  req->utf = false;
  req->range = 1;
  req->keys = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(origin_sym)) {
        String::Utf8Value origin(args[0]->ToObject()->Get(origin_sym));
        req->str = kc::strdup(*origin);
      }
      if (args[0]->ToObject()->Has(max_sym)) {
        req->max = args[0]->ToObject()->Get(max_sym)->NumberValue();
      }
      if (args[0]->ToObject()->Has(range_sym)) {
        req->range = args[0]->ToObject()->Get(range_sym)->NumberValue();
      }
      if (args[0]->ToObject()->Has(utf_sym)) {
        req->utf = args[0]->ToObject()->Get(utf_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(origin_sym)) {
      String::Utf8Value origin(args[0]->ToObject()->Get(origin_sym));
      req->str = kc::strdup(*origin);
    }
    if (args[0]->ToObject()->Has(max_sym)) {
      req->max = args[0]->ToObject()->Get(max_sym)->NumberValue();
    }
    if (args[0]->ToObject()->Has(range_sym)) {
      req->range = args[0]->ToObject()->Get(range_sym)->NumberValue();
    }
    if (args[0]->ToObject()->Has(utf_sym)) {
      req->utf = args[0]->ToObject()->Get(utf_sym)->BooleanValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

DEFINE_CHAR_PARAM_FUNC(Copy, KC_COPY);

Handle<Value> PolyDBWrap::Merge(const Arguments &args) {
  TRACE("Merge\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> srcary_sym = String::NewSymbol("srcary");
  Local<String> mode_sym = String::NewSymbol("mode");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(srcary_sym) && !args[0]->ToObject()->Get(srcary_sym)->IsArray()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(mode_sym) && !args[0]->ToObject()->Get(mode_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(srcary_sym) && !args[0]->ToObject()->Get(srcary_sym)->IsArray()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(mode_sym) && !args[0]->ToObject()->Get(mode_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_merge_req_t *req = (kc_merge_req_t *)malloc(sizeof(kc_merge_req_t));
  req->type = KC_MERGE;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->srcary = NULL;
  req->srcnum = -1;
  req->mode = PolyDB::MSET;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(srcary_sym)) {
        Local<Array> array = Local<Array>::Cast(args[0]->ToObject()->Get(srcary_sym));
        int32_t num = array->Length();
        req->srcary = new kc::BasicDB*[num];
        int32_t i = 0;
        for (i = 0; i < num; i++) {
          PolyDBWrap *dbwrap = ObjectWrap::Unwrap<PolyDBWrap>(Local<Object>::Cast(array->Get(Integer::New(i))));
          req->srcary[i] = dbwrap->db_;
        }
        req->srcnum = i;
      }
      if (args[0]->ToObject()->Has(mode_sym)) {
        req->mode = args[0]->ToObject()->Get(mode_sym)->NumberValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(srcary_sym)) {
      Local<Array> array = Local<Array>::Cast(args[0]->ToObject()->Get(srcary_sym));
      int32_t num = array->Length();
      req->srcary = new kc::BasicDB*[num];
      int32_t i = 0;
      for (i = 0; i < num; i++) {
        PolyDBWrap *dbwrap = ObjectWrap::Unwrap<PolyDBWrap>(Local<Object>::Cast(array->Get(Integer::New(i))));
        TRACE("dbwrap = %p, db_ = %p\n", dbwrap, dbwrap->db_);
        req->srcary[i] = dbwrap->db_;
      }
      req->srcnum = i;
    }
    if (args[0]->ToObject()->Has(mode_sym)) {
      req->mode = args[0]->ToObject()->Get(mode_sym)->NumberValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

DEFINE_CHAR_PARAM_FUNC(DumpSnapshot, KC_DUMP_SNAPSHOT);
DEFINE_CHAR_PARAM_FUNC(LoadSnapshot, KC_LOAD_SNAPSHOT);

Handle<Value> PolyDBWrap::Accept(const Arguments &args) {
  TRACE("Accept\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  Local<String> visitor_sym = String::NewSymbol("visitor");
  Local<String> writable_sym = String::NewSymbol("writable");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  PolyDB::Error::Code result = PolyDB::Error::SUCCESS;
  Local<Function> cb;
  bool writable = true;
  char *key = NULL;
  Local<Object> visitor;
  cb.Clear();
  visitor.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      cb = Local<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value _key(args[0]->ToObject()->Get(key_sym));
        key = kc::strdup(*_key);
      }
      if (args[0]->ToObject()->Has(visitor_sym)) {
        visitor = Local<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
      }
      if (args[0]->ToObject()->Has(writable_sym)) {
        writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value _key(args[0]->ToObject()->Get(key_sym));
      key = kc::strdup(*_key);
    }
    if (args[0]->ToObject()->Has(visitor_sym)) {
      visitor = Local<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
    }
    if (args[0]->ToObject()->Has(writable_sym)) {
      writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
    }
    cb = Local<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  // TODO' should be non-blocking implements ...
  // execute
  if (key == NULL || visitor.IsEmpty()) {
    result = PolyDB::Error::INVALID;
  } else {
    InternalVisitor _visitor(visitor, writable);
    if (!obj->db_->accept(key, strlen(key), &_visitor, writable)) {
      result = obj->db_->error().code();
    }
  }

  // init callback arguments.
  Local<Value> argv[1] = { 
    Local<Value>::New(Null()),
  };

  // set error to callback arguments.
  if (result != PolyDB::Error::SUCCESS) {
    const char *name = PolyDB::Error::codename(result);
    Local<String> message = String::NewSymbol(name);
    Local<Value> err = Exception::Error(message);
    Local<Object> obj = err->ToObject();
    obj->Set(String::NewSymbol("code"), Integer::New(result), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
    argv[0] = err;
  }

  // execute callback
  if (!cb.IsEmpty()) {
    TryCatch try_catch;
    MakeCallback(obj->handle_, cb, 1, argv);
    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }
  } 

  if (key) {
    free(key);
  }
  /*
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  */
  return scope.Close(args.This());
}

Handle<Value> PolyDBWrap::AcceptBulk(const Arguments &args) {
  TRACE("AcceptBulk\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> keys_sym = String::NewSymbol("keys");
  Local<String> visitor_sym = String::NewSymbol("visitor");
  Local<String> writable_sym = String::NewSymbol("writable");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && !args[0]->ToObject()->Get(keys_sym)->IsArray()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && !args[0]->ToObject()->Get(keys_sym)->IsArray()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  PolyDB::Error::Code result = PolyDB::Error::SUCCESS;
  Local<Function> cb;
  bool writable = true;
  StringVector *keys = NULL;
  Local<Object> visitor;
  cb.Clear();
  visitor.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      cb = Local<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(keys_sym)) {
        keys = Array2Vector(args[0]->ToObject()->Get(keys_sym));
      }
      if (args[0]->ToObject()->Has(visitor_sym)) {
        visitor = Local<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
      }
      if (args[0]->ToObject()->Has(writable_sym)) {
        writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(keys_sym)) {
      keys = Array2Vector(args[0]->ToObject()->Get(keys_sym));
    }
    if (args[0]->ToObject()->Has(visitor_sym)) {
      visitor = Local<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
    }
    if (args[0]->ToObject()->Has(writable_sym)) {
      writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
    }
    cb = Local<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  // TODO: should be non-blocking implements ...
  // execute
  if (keys == NULL || visitor.IsEmpty()) {
    result = PolyDB::Error::INVALID;
  } else {
    InternalVisitor _visitor(visitor, writable);
    if (!obj->db_->accept_bulk(*keys, &_visitor, writable)) {
      result = obj->db_->error().code();
    }
  }

  // init callback arguments.
  Local<Value> argv[1] = { 
    Local<Value>::New(Null()),
  };

  // set error to callback arguments.
  if (result != PolyDB::Error::SUCCESS) {
    const char *name = PolyDB::Error::codename(result);
    Local<String> message = String::NewSymbol(name);
    Local<Value> err = Exception::Error(message);
    Local<Object> obj = err->ToObject();
    obj->Set(String::NewSymbol("code"), Integer::New(result), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
    argv[0] = err;
  }


  // execute callback
  if (!cb.IsEmpty()) {
    TryCatch try_catch;
    MakeCallback(obj->handle_, cb, 1, argv);
    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }
  } 

  if (keys) {
    delete keys;
  }

  return scope.Close(args.This());
}

DEFINE_BOOL_PARAM_FUNC(BeginTransaction, begin_transaction, false)
DEFINE_BOOL_PARAM_FUNC(EndTransaction, end_transaction, true)


void PolyDBWrap::OnWork(uv_work_t *work_req) {
  TRACE("argument: work_req=%p\n", work_req);

  kc_req_t *req = static_cast<kc_req_t *>(work_req->data);
  PolyDBWrap *wrapdb = req->wrapdb;
  assert(wrapdb != NULL);
  PolyDB *db = wrapdb->db_;
  assert(db != NULL);

  // do operation
  switch (req->type) {
    case KC_OPEN:
      {
        kc_open_req_t *open_req = static_cast<kc_open_req_t *>(work_req->data);
        TRACE("open: path = %s(%p), mode = %d\n", open_req->path, open_req->path, open_req->mode);
        if (!db->open(std::string(open_req->path), open_req->mode)) {
          req->result = db->error().code();
        }
        break;
      }
    case KC_CLOSE:
      DO_EXECUTE(close);
      break;
    case KC_SET:
      { DO_KV_EXECUTE(set, work_req); break; }
    case KC_GET:
      { DO_KV_V_RET_EXECUTE(get, work_req); break; }
    case KC_CLEAR:
      DO_EXECUTE(clear);
      break;
    case KC_ADD:
      { DO_KV_EXECUTE(add, work_req); break; }
    case KC_APPEND:
      { DO_KV_EXECUTE(append, work_req); break; }
    case KC_REMOVE:
      {
        kc_char_cmn_req_t *remove_req = static_cast<kc_char_cmn_req_t*>(work_req->data);
        TRACE("remove: key = %s\n", remove_req->str);
        if (remove_req->str == NULL) {
          req->result = PolyDB::Error::INVALID;
        } else {
          if (!db->remove(remove_req->str, strlen(remove_req->str))) {
            req->result = db->error().code();
          }
        }
        break;
      }
    case KC_REPLACE:
      { DO_KV_EXECUTE(replace, work_req); break; }
    case KC_SEIZE:
      { DO_KV_V_RET_EXECUTE(seize, work_req); break; }
    case KC_INCREMENT:
      {
        kc_increment_req_t *increment_req = static_cast<kc_increment_req_t *>(work_req->data);
        TRACE("increment: key = %s, num = %d, orig = %d\n", increment_req->key, increment_req->num, increment_req->orig);
        if (increment_req->key == NULL) {
          increment_req->result = PolyDB::Error::INVALID;
        } else {
          int64_t num = db->increment(increment_req->key, strlen(increment_req->key), increment_req->num, increment_req->orig);
          TRACE("db->increment: num = %d\n", num);
          if (num == kc::INT64MIN) {
            increment_req->result = db->error().code();
          } else {
            increment_req->num = num;
          }
        }
        break;
      }
    case KC_INCREMENT_DOUBLE:
      {
        kc_increment_double_req_t *inc_dbl_req = static_cast<kc_increment_double_req_t *>(work_req->data);
        TRACE("increment_double: key = %s, num = %f, orig = %f\n", inc_dbl_req->key, inc_dbl_req->num, inc_dbl_req->orig);
        if (inc_dbl_req->key == NULL) {
          inc_dbl_req->result = PolyDB::Error::INVALID;
        } else {
          double num = db->increment_double(inc_dbl_req->key, strlen(inc_dbl_req->key), inc_dbl_req->num, inc_dbl_req->orig);
          TRACE("db->increment_double: num = %f\n", num);
          if (kc::chknan(num)) {
            inc_dbl_req->result = db->error().code();
          } else {
            inc_dbl_req->num = num;
          }
        }
        break;
      }
    case KC_CAS:
      {
        kc_cas_req_t *cas_req = static_cast<kc_cas_req_t *>(work_req->data);
        TRACE("cas: key = %s, oval = %s, nval = %s\n", cas_req->key, cas_req->oval, cas_req->nval);
        if (cas_req->key == NULL) {
          cas_req->result = PolyDB::Error::INVALID;
        } else {
          size_t oval_len = (cas_req->oval != NULL ? strlen(cas_req->oval) : 0);
          size_t nval_len = (cas_req->nval != NULL ? strlen(cas_req->nval) : 0);
          if (!db->cas(cas_req->key, strlen(cas_req->key), cas_req->oval, oval_len, cas_req->nval, nval_len)) {
            cas_req->result = db->error().code();
          }
        }
        break;
      }
    case KC_COUNT:
      { DO_RET_EXECUTE(count, work_req); break; }
    case KC_SIZE:
      { DO_RET_EXECUTE(size, work_req); break; }
    case KC_STATUS:
      {
        kc_strmap_ret_req_t *strmap_ret_req = static_cast<kc_strmap_ret_req_t*>(work_req->data);
        strmap_ret_req->ret = new StringMap();
        if (!db->status(strmap_ret_req->ret)) {
          strmap_ret_req->result = db->error().code();
        }
        break;
      }
    case KC_CHECK:
      {
        kc_check_req_t *check_req = static_cast<kc_check_req_t*>(work_req->data);
        if (check_req->key == NULL) {
          check_req->result = PolyDB::Error::INVALID;
        } else {
          check_req->ret = db->check(check_req->key, strlen(check_req->key));
          if (check_req->ret == -1) {
            check_req->result = db->error().code();
          }
        }
        break;
      }
    case KC_GET_BULK:
      {
        kc_get_bulk_req_t *gb_req = static_cast<kc_get_bulk_req_t*>(work_req->data);
        if (gb_req->keys == NULL) {
          gb_req->result = PolyDB::Error::INVALID;
        } else {
          gb_req->recs = new StringMap();
          int64_t cnt = db->get_bulk(*gb_req->keys, gb_req->recs, gb_req->atomic);
          if (cnt <= 0) {
            gb_req->result = db->error().code();
          }
          TRACE("get_bulk: ret = %ld, error.code = %d\n", cnt, gb_req->result);
        }
        break;
      }
    case KC_SET_BULK:
      {
        kc_set_bulk_req_t *sb_req = static_cast<kc_set_bulk_req_t*>(work_req->data);
        if (sb_req->recs == NULL) {
          sb_req->result = PolyDB::Error::INVALID;
        } else {
          int64_t num = db->set_bulk(*sb_req->recs, sb_req->atomic);
          sb_req->num = num;
          TRACE("set_bulk: ret = %ld\n", num);
          if (num <= 0) {
            sb_req->result = db->error().code();
          }
        }
        break;
      }
    case KC_REMOVE_BULK:
      {
        kc_remove_bulk_req_t *rb_req = static_cast<kc_remove_bulk_req_t*>(work_req->data);
        if (rb_req->keys == NULL) {
          rb_req->result = PolyDB::Error::INVALID;
        } else {
          int64_t num = db->remove_bulk(*rb_req->keys, rb_req->atomic);
          rb_req->num = num;
          if (num <= 0) {
            rb_req->result = db->error().code();
          }
          TRACE("remove_bulk: ret = %ld\n", num);
        }
        break;
      }
    case KC_MATCH_PREFIX:
      { DO_MATCH_CMN_EXECUTE(match_prefix, work_req); break; }
    case KC_MATCH_REGEX:
      { DO_MATCH_CMN_EXECUTE(match_regex, work_req); break; }
    case KC_MATCH_SIMILAR:
      {
        kc_match_similar_req_t *ms_req = static_cast<kc_match_similar_req_t*>(work_req->data);
        if (ms_req->str == NULL) {
          ms_req->result = PolyDB::Error::INVALID;
        } else {
          ms_req->keys = new StringVector();
          int64_t num = db->match_similar(std::string(ms_req->str, strlen(ms_req->str)), ms_req->range, ms_req->utf, ms_req->keys, ms_req->max);
          TRACE("match_similar: ret = %ld\n", num);
          if (num <= 0) {
            ms_req->result = db->error().code();
          }
        }
        break;
      }
    case KC_COPY:
      { DO_CHAR_PARAM_CMN_EXECUTE(copy, work_req); break; }
    case KC_MERGE:
      {
        kc_merge_req_t *merge_req = static_cast<kc_merge_req_t*>(work_req->data);
        TRACE("merge: srcary = %p, srcnum = %d, mode = %d\n", merge_req->srcary, merge_req->srcnum, merge_req->mode);
        if (merge_req->srcary == NULL || merge_req->srcnum == -1 ||
            !(merge_req->mode == PolyDB::MSET || 
              merge_req->mode == PolyDB::MADD || 
              merge_req->mode == PolyDB::MAPPEND)) {
          req->result = PolyDB::Error::INVALID;
        } else {
          if (!db->merge(merge_req->srcary, merge_req->srcnum, (PolyDB::MergeMode)merge_req->mode)) {
            req->result = db->error().code();
          }
        }
        break;
      }
    case KC_DUMP_SNAPSHOT:
      { DO_CHAR_PARAM_CMN_EXECUTE(dump_snapshot, work_req); break; }
    case KC_LOAD_SNAPSHOT:
      { DO_CHAR_PARAM_CMN_EXECUTE(load_snapshot, work_req); break; }
      /*
    case KC_ACCEPT:
      {
        kc_accept_req_t *accept_req = static_cast<kc_accept_req_t*>(work_req->data);
        TRACE("accept: key = %s, visitor = %p, writable = %d\n", accept_req->key, &accept_req->visitor, accept_req->writable);
        if (accept_req->key == NULL || accept_req->visitor.IsEmpty()) {
          req->result = PolyDB::Error::INVALID;
        } else {
          InternalVisitor visitor(accept_req->visitor, accept_req->writable);
          TRACE("accept...\n");
          if (!db->accept(accept_req->key, strlen(accept_req->key), &visitor, accept_req->writable)) {
            req->result = db->error().code();
          }
          TRACE("done accept\n");
        }
        break;
      }
    case KC_BEGIN_TRANSACTION:
      { DO_BOOL_PARAM_CMN_EXECUTE(begin_transaction, work_req); break; }
    case KC_END_TRANSACTION:
      { DO_BOOL_PARAM_CMN_EXECUTE(end_transaction, work_req); break; }
      */
    default:
      assert(0);
  }
}


Local<Value> PolyDBWrap::MakeErrorObject(PolyDB::Error::Code result) {
  const char *name = PolyDB::Error::codename(result);
  Local<String> message = String::NewSymbol(name);
  Local<Value> err = Exception::Error(message);
  Local<Object> obj = err->ToObject();
  obj->Set(String::NewSymbol("code"), Integer::New(result), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  return err;
}

void PolyDBWrap::OnWorkDone(uv_work_t *work_req) {
  TRACE("argument: work_req=%p\n", work_req);
  HandleScope scope;

  kc_req_t *req = static_cast<kc_req_t *>(work_req->data);
  PolyDBWrap *wrapdb = req->wrapdb;
  assert(wrapdb != NULL);

  // init callback arguments.
  int argc = 0;
  Local<Value> argv[2] = { 
    Local<Value>::New(Null()),
    Local<Value>::New(Null()),
  };

  // set error to callback arguments.
  if (req->result != PolyDB::Error::SUCCESS) {
    const char *name = PolyDB::Error::codename(req->result);
    Local<String> message = String::NewSymbol(name);
    Local<Value> err = Exception::Error(message);
    Local<Object> obj = err->ToObject();
    obj->Set(String::NewSymbol("code"), Integer::New(req->result), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
    argv[argc] = err;
  }
  argc++;

  // other data to callback argument.
  switch (req->type) {
    case KC_GET:
    case KC_SEIZE:
      {
        kc_kv_req_t *kv_req = static_cast<kc_kv_req_t *>(work_req->data);
        if (kv_req->value) {
          argv[argc++] = String::New(kv_req->value, strlen(kv_req->value));
        }
        break;
      }
    case KC_INCREMENT:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_increment_req_t *inc_req = static_cast<kc_increment_req_t *>(work_req->data);
          TRACE("increment->num = %d\n", inc_req->num);
          argv[argc++] = Integer::New(inc_req->num);
        }
        break;
      }
    case KC_INCREMENT_DOUBLE:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_increment_double_req_t *inc_dbl_req = static_cast<kc_increment_double_req_t *>(work_req->data);
          TRACE("increment_double->num = %f\n", inc_dbl_req->num);
          argv[argc++] = Number::New(inc_dbl_req->num);
        }
        break;
      }
    case KC_COUNT:
    case KC_SIZE:
      {
        kc_ret_req_t *ret_req = static_cast<kc_ret_req_t*>(work_req->data);
        argv[argc++] = Number::New(ret_req->ret);
        break;
      }
    case KC_STATUS:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_strmap_ret_req_t *strmap_ret_req = static_cast<kc_strmap_ret_req_t*>(work_req->data);
          argv[argc++] = Map2Obj(strmap_ret_req->ret);
        }
        break;
      }
    case KC_CHECK:
      {
        kc_check_req_t *check_req = static_cast<kc_check_req_t*>(work_req->data);
        argv[argc++] = Number::New(check_req->ret);
        break;
      }
    case KC_GET_BULK:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_get_bulk_req_t *gb_req = static_cast<kc_get_bulk_req_t*>(work_req->data);
          argv[argc++] = Map2Obj(gb_req->recs);
        }
        break;
      }
    case KC_SET_BULK:
      {
        kc_set_bulk_req_t *sb_req = static_cast<kc_set_bulk_req_t*>(work_req->data);
        argv[argc++] = Number::New(sb_req->num);
        break;
      }
    case KC_REMOVE_BULK:
      {
        kc_remove_bulk_req_t *rb_req = static_cast<kc_remove_bulk_req_t*>(work_req->data);
        argv[argc++] = Number::New(rb_req->num);
        break;
      }
    case KC_MATCH_PREFIX:
    case KC_MATCH_REGEX:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_match_cmn_req_t *m_req = static_cast<kc_match_cmn_req_t*>(work_req->data);
          argv[argc++] = Vector2Array(m_req->keys);
        }
        break;
      }
    case KC_MATCH_SIMILAR:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_match_similar_req_t *ms_req = static_cast<kc_match_similar_req_t*>(work_req->data);
          argv[argc++] = Vector2Array(ms_req->keys);
        }
        break;
      }
    default:
      break;
  }

  // execute callback
  if (!req->cb.IsEmpty()) {
    TryCatch try_catch;
    //req->cb->Call(Context::GetCurrent()->Global(), 1, argv);
    //TRACE("calling callback %d %p \n", req->type, work_req);
    MakeCallback(wrapdb->handle_, req->cb, argc, argv);
    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }
  } 


  // releases

  wrapdb->Unref();
  req->cb.Dispose();
  req->wrapdb = NULL;

  switch (req->type) {
    case KC_OPEN:
      {
        kc_open_req_t *open_req = static_cast<kc_open_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(open_req, path);
        free(open_req);
        break;
      }
    case KC_CLOSE:
    case KC_CLEAR:
    case KC_COUNT:
    case KC_SIZE:
      {
        kc_req_t *base_req = static_cast<kc_req_t*>(work_req->data);
        free(base_req);
        break;
      }
    case KC_STATUS:
      {
        kc_strmap_ret_req_t *strmap_ret_req = static_cast<kc_strmap_ret_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(strmap_ret_req, ret);
        free(strmap_ret_req);
        break;
      }
    case KC_SET:
    case KC_GET:
    case KC_ADD:
    case KC_APPEND:
    case KC_REPLACE:
    case KC_SEIZE:
      {
        kc_kv_req_t *kv_req = static_cast<kc_kv_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(kv_req, key);
        SAFE_REQ_ATTR_FREE(kv_req, value);
        free(kv_req);
        break;
      }
    case KC_INCREMENT:
      { 
        kc_increment_req_t *inc_req = static_cast<kc_increment_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(inc_req, key);
        free(inc_req);
        break;
      }
    case KC_INCREMENT_DOUBLE:
      { 
        kc_increment_double_req_t *inc_dbl_req = static_cast<kc_increment_double_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(inc_dbl_req, key);
        free(inc_dbl_req);
        break;
      }
    case KC_CAS:
      {
        kc_cas_req_t *cas_req = static_cast<kc_cas_req_t *>(work_req->data);
        SAFE_REQ_ATTR_FREE(cas_req, key);
        SAFE_REQ_ATTR_FREE(cas_req, oval);
        SAFE_REQ_ATTR_FREE(cas_req, nval);
        free(cas_req);
        break;
      }
    case KC_CHECK:
      { 
        kc_check_req_t *check_req = static_cast<kc_check_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(check_req, key);
        free(check_req);
        break;
      }
    case KC_GET_BULK:
      {
        kc_get_bulk_req_t *gb_req = static_cast<kc_get_bulk_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(gb_req, keys);
        SAFE_REQ_ATTR_DELETE(gb_req, recs);
        free(gb_req);
        break;
      }
    case KC_SET_BULK:
      {
        kc_set_bulk_req_t *sb_req = static_cast<kc_set_bulk_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(sb_req, recs);
        free(sb_req);
        break;
      }
    case KC_REMOVE_BULK:
      {
        kc_remove_bulk_req_t *rb_req = static_cast<kc_remove_bulk_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(rb_req, keys);
        free(rb_req);
        break;
      }
    case KC_MATCH_PREFIX:
    case KC_MATCH_REGEX:
      {
        kc_match_cmn_req_t *m_req = static_cast<kc_match_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(m_req, str);
        SAFE_REQ_ATTR_DELETE(m_req, keys);
        free(m_req);
        break;
      }
    case KC_MATCH_SIMILAR:
      {
        kc_match_similar_req_t *ms_req = static_cast<kc_match_similar_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(ms_req, str);
        SAFE_REQ_ATTR_DELETE(ms_req, keys);
        free(ms_req);
        break;
      }
    case KC_MERGE:
      {
        kc_merge_req_t *merge_req = static_cast<kc_merge_req_t*>(work_req->data);
        if (merge_req->srcary) {
          delete[] merge_req->srcary;
          merge_req->srcary = NULL;
        }
        free(merge_req);
        break;
      }
    case KC_REMOVE:
    case KC_COPY:
    case KC_DUMP_SNAPSHOT:
    case KC_LOAD_SNAPSHOT:
      {
        kc_char_cmn_req_t *path_req = static_cast<kc_char_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(path_req, str);
        free(path_req);
        break;
      }
      /*
    case KC_ACCEPT:
      {
        kc_accept_req_t *accept_req = static_cast<kc_accept_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(accept_req, key);
        free(accept_req);
        break;
      }
    case KC_BEGIN_TRANSACTION:
    case KC_END_TRANSACTION:
      {
        kc_boolean_cmn_req_t *tran_req = static_cast<kc_boolean_cmn_req_t*>(work_req->data);
        free(tran_req);
        break;
      }
      */
    default:
      assert(0);
  }
  work_req->data = NULL;

  free(work_req);
}

void PolyDBWrap::Init(Handle<Object> target) {
  TRACE("load kyotocabinet module\n");

  // prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("DB"));

  Local<ObjectTemplate> insttpl = tpl->InstanceTemplate();
  insttpl->SetInternalFieldCount(1);

  // prototype
  Local<ObjectTemplate> prottpl = tpl->PrototypeTemplate();
  prottpl->Set(String::NewSymbol("open"), FunctionTemplate::New(Open)->GetFunction());
  prottpl->Set(String::NewSymbol("close"), FunctionTemplate::New(Close)->GetFunction());
  prottpl->Set(String::NewSymbol("set"), FunctionTemplate::New(Set)->GetFunction());
  prottpl->Set(String::NewSymbol("get"), FunctionTemplate::New(Get)->GetFunction());
  prottpl->Set(String::NewSymbol("clear"), FunctionTemplate::New(Clear)->GetFunction());
  prottpl->Set(String::NewSymbol("add"), FunctionTemplate::New(Add)->GetFunction());
  prottpl->Set(String::NewSymbol("append"), FunctionTemplate::New(Append)->GetFunction());
  prottpl->Set(String::NewSymbol("remove"), FunctionTemplate::New(Remove)->GetFunction());
  prottpl->Set(String::NewSymbol("replace"), FunctionTemplate::New(Replace)->GetFunction());
  prottpl->Set(String::NewSymbol("seize"), FunctionTemplate::New(Seize)->GetFunction());
  prottpl->Set(String::NewSymbol("increment"), FunctionTemplate::New(Increment)->GetFunction());
  prottpl->Set(String::NewSymbol("increment_double"), FunctionTemplate::New(IncrementDouble)->GetFunction());
  prottpl->Set(String::NewSymbol("cas"), FunctionTemplate::New(Cas)->GetFunction());
  prottpl->Set(String::NewSymbol("count"), FunctionTemplate::New(Count)->GetFunction());
  prottpl->Set(String::NewSymbol("size"), FunctionTemplate::New(Size)->GetFunction());
  prottpl->Set(String::NewSymbol("status"), FunctionTemplate::New(Status)->GetFunction());
  prottpl->Set(String::NewSymbol("check"), FunctionTemplate::New(Check)->GetFunction());
  prottpl->Set(String::NewSymbol("get_bulk"), FunctionTemplate::New(GetBulk)->GetFunction());
  prottpl->Set(String::NewSymbol("set_bulk"), FunctionTemplate::New(SetBulk)->GetFunction());
  prottpl->Set(String::NewSymbol("remove_bulk"), FunctionTemplate::New(RemoveBulk)->GetFunction());
  prottpl->Set(String::NewSymbol("match_prefix"), FunctionTemplate::New(MatchPrefix)->GetFunction());
  prottpl->Set(String::NewSymbol("match_regex"), FunctionTemplate::New(MatchRegex)->GetFunction());
  prottpl->Set(String::NewSymbol("match_similar"), FunctionTemplate::New(MatchSimilar)->GetFunction());
  prottpl->Set(String::NewSymbol("copy"), FunctionTemplate::New(Copy)->GetFunction());
  prottpl->Set(String::NewSymbol("merge"), FunctionTemplate::New(Merge)->GetFunction());
  prottpl->Set(String::NewSymbol("dump_snapshot"), FunctionTemplate::New(DumpSnapshot)->GetFunction());
  prottpl->Set(String::NewSymbol("load_snapshot"), FunctionTemplate::New(LoadSnapshot)->GetFunction());
  prottpl->Set(String::NewSymbol("accept"), FunctionTemplate::New(Accept)->GetFunction());
  prottpl->Set(String::NewSymbol("accept_bulk"), FunctionTemplate::New(AcceptBulk)->GetFunction());
  prottpl->Set(String::NewSymbol("begin_transaction"), FunctionTemplate::New(BeginTransaction)->GetFunction());
  prottpl->Set(String::NewSymbol("end_transaction"), FunctionTemplate::New(EndTransaction)->GetFunction());

  Persistent<Function> ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("DB"), ctor);

  // define OpenMode
  ctor->Set(String::NewSymbol("OREADER"), Integer::New(PolyDB::OREADER), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("OWRITER"), Integer::New(PolyDB::OWRITER), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("OCREATE"), Integer::New(PolyDB::OCREATE), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("OTRUNCATE"), Integer::New(PolyDB::OTRUNCATE), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("OAUTOTRAN"), Integer::New(PolyDB::OAUTOTRAN), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("OAUTOSYNC"), Integer::New(PolyDB::OAUTOSYNC), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("ONOLOCK"), Integer::New(PolyDB::ONOLOCK), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("OTRYLOCK"), Integer::New(PolyDB::OTRYLOCK), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("ONOREPAIR"), Integer::New(PolyDB::ONOREPAIR), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  // define MergeMode
  ctor->Set(String::NewSymbol("MSET"), Integer::New(PolyDB::MSET), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("MADD"), Integer::New(PolyDB::MADD), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("MREPLACE"), Integer::New(PolyDB::MREPLACE), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("MAPPEND"), Integer::New(PolyDB::MAPPEND), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  // define MapReduce Option
  ctor->Set(String::NewSymbol("XNOLOCK"), Integer::New(MapReduce::XNOLOCK), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("XPARAMAP"), Integer::New(MapReduce::XPARAMAP), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("XPARARED"), Integer::New(MapReduce::XPARARED), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("XPARAFLS"), Integer::New(MapReduce::XPARAFLS), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("XNOCOMP"), Integer::New(MapReduce::XNOCOMP), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
}

