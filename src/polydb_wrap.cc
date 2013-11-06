/*
 * polydb wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include "polydb_wrap.h"
#include "cursor_wrap.h"
#include "async.h"
#include <assert.h>
#include "debug.h"
#include "utils.h"
#include <kcdbext.h>
#include <kcdb.h>


using namespace v8;
namespace kc = kyotocabinet;


#define DEFINE_FUNC(Name, Type)                                                     \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                           \
    HandleScope scope;                                                              \
    TRACE("%s\n", #Name);                                                           \
                                                                                    \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());                  \
    assert(obj != NULL);                                                            \
                                                                                    \
    if ((args.Length() == 1 && (!args[0]->IsObject())                               \
                              | !args[0]->IsFunction())) {                          \
      ThrowException(Exception::TypeError(String::New("Bad argument")));            \
      return scope.Close(args.This());                                              \
    }                                                                               \
                                                                                    \
    kc_req_t *req = reinterpret_cast<kc_req_t*>(malloc(sizeof(kc_req_t)));          \
    assert(req != NULL);                                                            \
    req->type = Type;                                                               \
    req->result = PolyDB::Error::SUCCESS;                                           \
    req->wrapdb = obj;                                                              \
    req->cb.Clear();                                                                \
                                                                                    \
    if (args.Length() > 0 && args[0]->IsFunction()) {                               \
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));         \
    }                                                                               \
                                                                                    \ 
    SendAsyncRequest(req);                                                          \
                                                                                    \
    obj->Ref();                                                                     \
    return scope.Close(args.This());                                                \
  }                                                                                 \

#define DEFINE_CHAR_PARAM_FUNC(Name, Type)                                            \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                             \
    HandleScope scope;                                                                \
    TRACE("%s\n", #Name);                                                             \
                                                                                      \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());                    \
    assert(obj != NULL);                                                              \
                                                                                      \
    if ( (args.Length() == 0) ||                                                      \
         (args.Length() == 1 && (!args[0]->IsString()) | !args[0]->IsFunction()) ) {  \
      ThrowException(Exception::TypeError(String::New("Bad argument")));              \
      return scope.Close(args.This());                                                \
    }                                                                                 \
                                                                                      \
    kc_char_cmn_req_t *req =                                                          \
      reinterpret_cast<kc_char_cmn_req_t*>(malloc(sizeof(kc_char_cmn_req_t)));        \
    assert(req != NULL);                                                              \
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
    SendAsyncRequest(req);                                                            \
                                                                                      \
    obj->Ref();                                                                       \
    return scope.Close(args.This());                                                  \
  }                                                                                   \

#define DEFINE_BOOL_PARAM_FUNC(Name, Method, INIT_VALUE)                        \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                       \
    HandleScope scope;                                                          \
    TRACE("%s\n", #Name);                                                       \
                                                                                \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());              \
    assert(obj != NULL);                                                        \
                                                                                \
    if ( (args.Length() == 0) ||                                                \
         (args.Length() == 1 && (!args[0]->IsBoolean()) &                       \
                                 !args[0]->IsFunction()) ) {                    \
      ThrowException(Exception::TypeError(String::New("Bad argument")));        \
      return scope.Close(args.This());                                          \
    }                                                                           \
                                                                                \ 
    PolyDB::Error::Code result = PolyDB::Error::SUCCESS;                        \
    Local<Function> cb;                                                         \
    bool flag = INIT_VALUE;                                                     \
    cb.Clear();                                                                 \
                                                                                \ 
    if (args.Length() == 1) {                                                   \
      if (args[0]->IsFunction()) {                                              \
        cb = Local<Function>::New(Handle<Function>::Cast(args[0]));             \
      } else if (args[0]->IsBoolean()) {                                        \
        flag = args[0]->BooleanValue();                                         \
      }                                                                         \
    } else {                                                                    \
      flag = args[0]->BooleanValue();                                           \
      cb = Local<Function>::New(Handle<Function>::Cast(args[1]));               \
    }                                                                           \
                                                                                \
    TRACE("call %s: flag = %d\n", #Method, flag);                               \
    if (!obj->db_->Method(flag)) {                                              \
      result = obj->db_->error().code();                                        \
    }                                                                           \
                                                                                \ 
    Local<Value> argv[1] = {                                                    \
      Local<Value>::New(Null()),                                                \
    };                                                                          \
                                                                                \ 
    if (result != PolyDB::Error::SUCCESS) {                                     \
      const char *name = PolyDB::Error::codename(result);                       \
      Local<String> message = String::NewSymbol(name);                          \
      Local<Value> err = Exception::Error(message);                             \
      Local<Object> obj = err->ToObject();                                      \
      obj->Set(                                                                 \
        String::NewSymbol("code"), Integer::New(result),                        \
        static_cast<PropertyAttribute>(ReadOnly | DontDelete)                   \
      );                                                                        \
      argv[0] = err;                                                            \
    }                                                                           \
                                                                                \ 
    if (!cb.IsEmpty()) {                                                        \
      TryCatch try_catch;                                                       \
      MakeCallback(obj->handle_, cb, 1, argv);                                  \
      if (try_catch.HasCaught()) {                                              \
        FatalException(try_catch);                                              \
      }                                                                         \
    }                                                                           \ 
                                                                                \ 
    return scope.Close(args.This());                                            \
  }                                                                             \

#define DEFINE_RET_FUNC(Name, Type, REQ_TYPE, INIT_VALUE)                           \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                           \
    HandleScope scope;                                                              \
    TRACE("%s\n", #Name);                                                           \
                                                                                    \
    PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());                  \
    assert(obj != NULL);                                                            \
                                                                                    \
    if ((args.Length() == 1 && (!args[0]->IsObject())                               \
                              | !args[0]->IsFunction())) {                          \
      ThrowException(Exception::TypeError(String::New("Bad argument")));            \
      return scope.Close(args.This());                                              \
    }                                                                               \
                                                                                    \
    REQ_TYPE *req = reinterpret_cast<REQ_TYPE*>(malloc(sizeof(REQ_TYPE)));          \
    req->type = Type;                                                               \
    req->result = PolyDB::Error::SUCCESS;                                           \
    req->wrapdb = obj;                                                              \
    req->cb.Clear();                                                                \
    req->ret = INIT_VALUE;                                                          \
                                                                                    \
    if (args.Length() > 0 && args[0]->IsFunction()) {                               \
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));         \
    }                                                                               \
                                                                                    \ 
    SendAsyncRequest(req);                                                          \
                                                                                    \
    obj->Ref();                                                                     \
    return scope.Close(args.This());                                                \
  }                                                                                 \

#define DEFINE_KV_K_ONLY(Name, Type)                                            \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                       \
    HandleScope scope;                                                          \
    TRACE("%s\n", #Name);                                                       \
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
      return scope.Close(args.This());                                          \
    }                                                                           \
                                                                                \
    kc_kv_req_t *req =                                                          \
      reinterpret_cast<kc_kv_req_t*>(malloc(sizeof(kc_kv_req_t)));              \
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
    SendAsyncRequest(req);                                                      \
                                                                                \
    obj->Ref();                                                                 \
    return scope.Close(args.This());                                            \
  }                                                                             \

#define DEFINE_KV_FUNC(Name, Type)                                              \
  Handle<Value> PolyDBWrap::Name(const Arguments &args) {                       \
    HandleScope scope;                                                          \
    TRACE("%s\n", #Name);                                                       \
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
      return scope.Close(args.This());                                          \
    }                                                                           \
                                                                                \
    kc_kv_req_t *req =                                                          \
      reinterpret_cast<kc_kv_req_t*>(malloc(sizeof(kc_kv_req_t)));              \
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
    SendAsyncRequest(req);                                                      \
                                                                                \
    obj->Ref();                                                                 \
    return scope.Close(args.This());                                            \
  }                                                                             \

#define DO_EXECUTE(Method)              \
  if (!db->Method()) {                  \
    req->result = db->error().code();   \
  }                                     \

#define DO_CHAR_PARAM_CMN_EXECUTE(Method, WorkReq)                              \
  kc_char_cmn_req_t *req = reinterpret_cast<kc_char_cmn_req_t*>(WorkReq->data); \
  if (req->str == NULL) {                                                       \
    req->result = PolyDB::Error::INVALID;                                       \
  } else {                                                                      \
    if (!db->Method(req->str)) {                                                \
      req->result = db->error().code();                                         \
    }                                                                           \
  }                                                                             \

#define DO_BOOL_PARAM_CMN_EXECUTE(Method, WorkReq)                                     \
  kc_boolean_cmn_req_t *req = reinterpret_cast<kc_boolean_cmn_req_t*>(WorkReq->data);  \
  if (!db->Method(req->flag)) {                                                        \
    req->result = db->error().code();                                                  \
  }                                                                                    \

#define DO_RET_EXECUTE(Method, WorkReq)                                  \
  kc_ret_req_t *req = reinterpret_cast<kc_ret_req_t*>(WorkReq->data);    \
  req->ret = db->Method();                                               \
  TRACE("%s: ret = %d\n", #Method, req->ret);                            \
  if (req->ret == -1) {                                                  \
    req->result = db->error().code();                                    \
  }                                                                      \

#define DO_KV_V_RET_EXECUTE(Method, WorkReq)                                         \
  kc_kv_req_t *req = reinterpret_cast<kc_kv_req_t*>(WorkReq->data);                  \
  size_t value_size;                                                                 \
  if (req->key == NULL) {                                                            \
    req->result = PolyDB::Error::INVALID;                                            \
  } else {                                                                           \
    req->value = db->Method(req->key, strlen(req->key), &value_size);                \
    TRACE("%s: return value = %s, size = %d\n", #Method, req->value, value_size);    \
    if (req->value == NULL) {                                                        \
      req->result = db->error().code();                                              \
    }                                                                                \
  }                                                                                  \

#define DO_KV_EXECUTE(Method, WorkReq)                                 \
  kc_kv_req_t *req = reinterpret_cast<kc_kv_req_t*>(WorkReq->data);    \
  TRACE("key = %s, value = %s\n", req->key, req->value);               \
  if (req->key == NULL || req->value == NULL) {                        \
    req->result = PolyDB::Error::INVALID;                              \
  } else {                                                             \
    if (!db->Method(req->key, strlen(req->key),                        \
                    req->value, strlen(req->value))) {                 \
      req->result = db->error().code();                                \
    }                                                                  \
  }                                                                    \

#define DO_MATCH_CMN_EXECUTE(Method, WorkReq)                                               \
  kc_match_cmn_req_t *req = reinterpret_cast<kc_match_cmn_req_t*>(WorkReq->data);           \
  if (req->str == NULL) {                                                                   \
    req->result = PolyDB::Error::INVALID;                                                   \
  } else {                                                                                  \
    req->keys = new StringVector();                                                         \
    int64_t num = db->Method(std::string(req->str, strlen(req->str)), req->keys, req->max); \
    TRACE("match return: %lld\n", num);                                                     \
    if (num <= 0) {                                                                         \
      req->result = db->error().code();                                                     \
    }                                                                                       \
  }                                                                                         \


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
  KC_PATH,
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
  KC_ITERATE,
  KC_BEGIN_TRANSACTION,
  KC_END_TRANSACTION,
  KC_SYNCHRONIZE,
  KC_OCCUPY,
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
typedef struct kc_open_req_s {
  KC_REQ_FIELD;
  char *path;
  uint32_t mode;
} kc_open_req_t;

// key/value params common request
typedef struct kc_kv_req_s {
  KC_REQ_FIELD;
  char *key;
  char *value;
} kc_kv_req_t;

// increment request
typedef struct kc_increment_req_s {
  KC_REQ_FIELD;
  char *key;
  int64_t num;
  int64_t orig;
} kc_increment_req_t;

// increment_double request
typedef struct kc_increment_double_req_s {
  KC_REQ_FIELD;
  char *key;
  double num;
  double orig;
} kc_increment_double_req_t;

// cas request
typedef struct kc_cas_req_s {
  KC_REQ_FIELD;
  char *key;
  char *oval;
  char *nval;
} kc_cas_req_t;

// return value common request
typedef struct kc_ret_req_s {
  KC_REQ_FIELD;
  int64_t ret;
} kc_ret_req_t;

// string map value request
typedef struct kc_strmap_ret_req_s {
  KC_REQ_FIELD;
  StringMap *ret;
} kc_strmap_ret_req_t;

// char value request
typedef struct kc_char_ret_req_s {
  KC_REQ_FIELD;
  char *ret;
} kc_char_ret_req_t;

// check request
typedef struct kc_check_req_s {
  KC_REQ_FIELD;
  char *key;
  int64_t ret;
} kc_check_req_t;

// get_bulk request
typedef struct kc_get_bulk_req_s {
  KC_REQ_FIELD;
  StringVector *keys;
  bool atomic;
  StringMap *recs;
} kc_get_bulk_req_t;

// set_bulk request
typedef struct kc_set_bulk_req_s {
  KC_REQ_FIELD;
  StringMap *recs;
  bool atomic;
  int64_t num;
} kc_set_bulk_req_t;

// remove_bulk request
typedef struct kc_remove_bulk_req_s {
  KC_REQ_FIELD;
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
typedef struct kc_match_cmn_req_s {
  KC_REQ_FIELD;
  KC_MATCH_REQ_FIELD;
} kc_match_cmn_req_t;

// match_similar request
typedef struct kc_match_similar_req_s {
  KC_REQ_FIELD;
  KC_MATCH_REQ_FIELD;
  int64_t range;
  bool utf;
} kc_match_similar_req_t;

// charactor pointer type parameter common request
typedef struct kc_char_cmn_req_s {
  KC_REQ_FIELD;
  char *str;
} kc_char_cmn_req_t;

// merge request
typedef struct kc_merge_req_s {
  KC_REQ_FIELD;
  kc::BasicDB **srcary;
  int32_t srcnum;
  uint32_t mode;
} kc_merge_req_t;

// accept request
typedef struct kc_accept_req_s {
  KC_REQ_FIELD;
  char *key;
  Persistent<Object> visitor;
  bool writable;
} kc_accept_req_t;

// accept bulk request
typedef struct kc_accept_bulk_req_s {
  KC_REQ_FIELD;
  StringVector *keys;
  Persistent<Object> visitor;
  bool writable;
} kc_accept_bulk_req_t;

// boolean parameter common request
typedef struct kc_boolean_cmn_req_s {
  KC_REQ_FIELD;
  bool flag;
} kc_boolean_cmn_req_t;

// file processor common request
typedef struct kc_file_processor_cmn_req_s {
  KC_REQ_FIELD;
  bool flag;
  Persistent<Object> proc;
} kc_file_processor_cmn_req_t;


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
  //mapreduce_ctor.Dispose();
  assert(db_ != NULL);
  TRACE("destor: db_ = %p\n", db_);
  delete db_;
  db_ = NULL;
}

PolyDB::Cursor* PolyDBWrap::Cursor() {
  assert(db_ != NULL);
  TRACE("db_ = %p\n", db_);
  return db_->cursor();
}

void PolyDBWrap::SendAsyncRequest(void *req) {
  assert(req != NULL);

  uv_work_t *uv_req = reinterpret_cast<uv_work_t*>(malloc(sizeof(uv_work_t)));
  assert(uv_req != NULL);
  uv_req->data = req;
  TRACE("uv_work_t = %p, type = %d\n", uv_req, ((kc_req_t *)req)->type);

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);
}


Handle<Value> PolyDBWrap::New(const Arguments &args) {
  HandleScope scope;
  TRACE("New\n");

  PolyDBWrap *obj = new PolyDBWrap();
  obj->Wrap(args.This());

  return scope.Close(args.This());
}

Handle<Value> PolyDBWrap::Open(const Arguments &args) {
  TRACE("Open\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  kc_open_req_t *open_req = reinterpret_cast<kc_open_req_t*>(malloc(sizeof(kc_open_req_t)));
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

  SendAsyncRequest(open_req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_char_cmn_req_t *req = 
    reinterpret_cast<kc_char_cmn_req_t*>(malloc(sizeof(kc_char_cmn_req_t)));
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
  
  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
}

DEFINE_KV_FUNC(Replace, KC_REPLACE);
DEFINE_KV_K_ONLY(Seize, KC_SEIZE);

Handle<Value> PolyDBWrap::Increment(const Arguments &args) {
  HandleScope scope;
  TRACE("Increment\n");

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  Local<String> num_sym = String::NewSymbol("num");
  Local<String> orig_sym = String::NewSymbol("orig");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) &&
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(num_sym) && 
                              !args[0]->ToObject()->Get(num_sym)->IsNumber()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(orig_sym) && 
                              !args[0]->ToObject()->Get(orig_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(num_sym) && 
                              !args[0]->ToObject()->Get(num_sym)->IsNumber()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(orig_sym) && 
                              !args[0]->ToObject()->Get(orig_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_increment_req_t *increment_req = 
    reinterpret_cast<kc_increment_req_t*>(malloc(sizeof(kc_increment_req_t)));
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
      if (kc::chkinf(orig)) {
        increment_req->orig = (orig >= 0 ? kc::INT64MAX : kc::INT64MIN);
      } else {
        increment_req->orig = orig;
      }
    }
    increment_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  SendAsyncRequest(increment_req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(num_sym) && 
                              !args[0]->ToObject()->Get(num_sym)->IsNumber()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(orig_sym) && 
                              !args[0]->ToObject()->Get(orig_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(num_sym) && 
                              !args[0]->ToObject()->Get(num_sym)->IsNumber()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(orig_sym) && 
                              !args[0]->ToObject()->Get(orig_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_increment_double_req_t *inc_dbl_req = 
    reinterpret_cast<kc_increment_double_req_t*>(malloc(sizeof(kc_increment_double_req_t)));
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
  
  SendAsyncRequest(inc_dbl_req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(oval_sym) && 
                              !args[0]->ToObject()->Get(oval_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(nval_sym) && 
                              !args[0]->ToObject()->Get(nval_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(oval_sym) && 
                              !args[0]->ToObject()->Get(oval_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(nval_sym) && 
                              !args[0]->ToObject()->Get(nval_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cas_req_t *cas_req = reinterpret_cast<kc_cas_req_t*>(malloc(sizeof(kc_cas_req_t)));
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
  
  SendAsyncRequest(cas_req);

  obj->Ref();
  return scope.Close(args.This());
}

DEFINE_RET_FUNC(Count, KC_COUNT, kc_ret_req_t, -1);
DEFINE_RET_FUNC(Size, KC_SIZE, kc_ret_req_t, -1);
DEFINE_RET_FUNC(Path, KC_PATH, kc_char_ret_req_t, NULL);
DEFINE_RET_FUNC(Status, KC_STATUS, kc_strmap_ret_req_t, NULL);

Handle<Value> PolyDBWrap::Check(const Arguments &args) {
  TRACE("Check\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_check_req_t *check_req = 
    reinterpret_cast<kc_check_req_t*>(malloc(sizeof(kc_check_req_t)));
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
  
  SendAsyncRequest(check_req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && 
                              !args[0]->ToObject()->Get(keys_sym)->IsArray()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && 
                              !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && 
                              !args[0]->ToObject()->Get(keys_sym)->IsArray()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && 
                              !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_get_bulk_req_t *req = 
    reinterpret_cast<kc_get_bulk_req_t*>(malloc(sizeof(kc_get_bulk_req_t)));
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
  
  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(recs_sym) && 
                              !args[0]->ToObject()->Get(recs_sym)->IsObject()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && 
                              !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(recs_sym) && 
                              !args[0]->ToObject()->Get(recs_sym)->IsObject()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && 
                              !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_set_bulk_req_t *req = 
    reinterpret_cast<kc_set_bulk_req_t*>(malloc(sizeof(kc_set_bulk_req_t)));
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
  
  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && 
                              !args[0]->ToObject()->Get(keys_sym)->IsArray()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && 
                              !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && 
                              !args[0]->ToObject()->Get(keys_sym)->IsArray()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(atomic_sym) && 
                              !args[0]->ToObject()->Get(atomic_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_remove_bulk_req_t *req = 
    reinterpret_cast<kc_remove_bulk_req_t*>(malloc(sizeof(kc_remove_bulk_req_t)));
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
  
  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(prefix_sym) && 
                              !args[0]->ToObject()->Get(prefix_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && 
                              !args[0]->ToObject()->Get(max_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(prefix_sym) && 
                              !args[0]->ToObject()->Get(prefix_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && 
                              !args[0]->ToObject()->Get(max_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_match_cmn_req_t *req = 
    reinterpret_cast<kc_match_cmn_req_t*>(malloc(sizeof(kc_match_cmn_req_t)));
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
  
  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(regex_sym) && 
                              !args[0]->ToObject()->Get(regex_sym)->IsRegExp()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && 
                              !args[0]->ToObject()->Get(max_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(regex_sym) && 
                              !args[0]->ToObject()->Get(regex_sym)->IsRegExp()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && 
                              !args[0]->ToObject()->Get(max_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_match_cmn_req_t *req = 
    reinterpret_cast<kc_match_cmn_req_t*>(malloc(sizeof(kc_match_cmn_req_t)));
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
  
  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(origin_sym) && 
                              !args[0]->ToObject()->Get(origin_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && 
                              !args[0]->ToObject()->Get(max_sym)->IsNumber()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(range_sym) && 
                              !args[0]->ToObject()->Get(range_sym)->IsNumber()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(utf_sym) && 
                              !args[0]->ToObject()->Get(utf_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(origin_sym) && 
                              !args[0]->ToObject()->Get(origin_sym)->IsString()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(max_sym) && 
                              !args[0]->ToObject()->Get(max_sym)->IsNumber()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(range_sym) && 
                              !args[0]->ToObject()->Get(range_sym)->IsNumber()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(utf_sym) && 
                              !args[0]->ToObject()->Get(utf_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_match_similar_req_t *req = 
    reinterpret_cast<kc_match_similar_req_t*>(malloc(sizeof(kc_match_similar_req_t)));
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
  
  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(srcary_sym) && 
                              !args[0]->ToObject()->Get(srcary_sym)->IsArray()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(mode_sym) && 
                              !args[0]->ToObject()->Get(mode_sym)->IsNumber()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(srcary_sym) && 
                              !args[0]->ToObject()->Get(srcary_sym)->IsArray()) || 
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(mode_sym) && 
                              !args[0]->ToObject()->Get(mode_sym)->IsNumber()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_merge_req_t *req = 
    reinterpret_cast<kc_merge_req_t*>(malloc(sizeof(kc_merge_req_t)));
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
  
  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && 
                              !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && 
                              !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && 
                              !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && 
                              !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && 
                              !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_accept_req_t *req = reinterpret_cast<kc_accept_req_t*>(malloc(sizeof(kc_accept_req_t)));
  req->type = KC_ACCEPT;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->key = NULL;
  req->writable = true;
  req->visitor.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value _key(args[0]->ToObject()->Get(key_sym));
        req->key = kc::strdup(*_key);
      }
      if (args[0]->ToObject()->Has(visitor_sym)) {
        req->visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
      }
      if (args[0]->ToObject()->Has(writable_sym)) {
        req->writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value _key(args[0]->ToObject()->Get(key_sym));
      req->key = kc::strdup(*_key);
    }
    if (args[0]->ToObject()->Has(visitor_sym)) {
      req->visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
    }
    if (args[0]->ToObject()->Has(writable_sym)) {
      req->writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  SendAsyncRequest(req);

  obj->Ref();
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
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && 
                              !args[0]->ToObject()->Get(keys_sym)->IsArray()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && 
                              !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && 
                              !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(keys_sym) && 
                              !args[0]->ToObject()->Get(keys_sym)->IsArray()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && 
                              !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && 
                              !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_accept_bulk_req_t *req = 
    reinterpret_cast<kc_accept_bulk_req_t*>(malloc(sizeof(kc_accept_bulk_req_t)));
  req->type = KC_ACCEPT_BULK;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->keys = NULL;
  req->writable = true;
  req->visitor.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(keys_sym)) {
        req->keys = Array2Vector(args[0]->ToObject()->Get(keys_sym));
      }
      if (args[0]->ToObject()->Has(visitor_sym)) {
        req->visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
      }
      if (args[0]->ToObject()->Has(writable_sym)) {
        req->writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(keys_sym)) {
      req->keys = Array2Vector(args[0]->ToObject()->Get(keys_sym));
    }
    if (args[0]->ToObject()->Has(visitor_sym)) {
      req->visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
    }
    if (args[0]->ToObject()->Has(writable_sym)) {
      req->writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
}

Handle<Value> PolyDBWrap::Iterate(const Arguments &args) {
  TRACE("Iterate\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> visitor_sym = String::NewSymbol("visitor");
  Local<String> writable_sym = String::NewSymbol("writable");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && 
                              !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && 
                              !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(visitor_sym) && 
                              !args[0]->ToObject()->Get(visitor_sym)->IsObject()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && 
                              !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_accept_req_t *req = reinterpret_cast<kc_accept_req_t*>(malloc(sizeof(kc_accept_req_t)));
  req->type = KC_ITERATE;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->cb.Clear();
  req->key = NULL;
  req->writable = true;
  req->visitor.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(visitor_sym)) {
        req->visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
      }
      if (args[0]->ToObject()->Has(writable_sym)) {
        req->writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(visitor_sym)) {
      req->visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
    }
    if (args[0]->ToObject()->Has(writable_sym)) {
      req->writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
}

DEFINE_BOOL_PARAM_FUNC(BeginTransaction, begin_transaction, false)
DEFINE_BOOL_PARAM_FUNC(EndTransaction, end_transaction, true)

Handle<Value> PolyDBWrap::Synchronize(const Arguments &args) {
  TRACE("Synchronize\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> proc_sym = String::NewSymbol("proc");
  Local<String> hard_sym = String::NewSymbol("hard");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) & !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(proc_sym) && 
                              !args[0]->ToObject()->Get(proc_sym)->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(hard_sym) && 
                              !args[0]->ToObject()->Get(hard_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(proc_sym) && 
                              !args[0]->ToObject()->Get(proc_sym)->IsFunction()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(hard_sym) && 
                              !args[0]->ToObject()->Get(hard_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_file_processor_cmn_req_t *req = 
    reinterpret_cast<kc_file_processor_cmn_req_t*>(malloc(sizeof(kc_file_processor_cmn_req_t)));
  req->type = KC_SYNCHRONIZE;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->flag = false;
  req->proc.Clear();
  req->cb.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(hard_sym)) {
        req->flag = args[0]->ToObject()->Get(hard_sym)->BooleanValue();
      }
      if (args[0]->ToObject()->Has(proc_sym)) {
        req->proc = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(proc_sym)));
      }
    }
  } else {
    if (args[0]->ToObject()->Has(hard_sym)) {
      req->flag = args[0]->ToObject()->Get(hard_sym)->BooleanValue();
    }
    if (args[0]->ToObject()->Has(proc_sym)) {
      req->proc = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(proc_sym)));
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
}

Handle<Value> PolyDBWrap::Occupy(const Arguments &args) {
  TRACE("Occupy\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> proc_sym = String::NewSymbol("proc");
  Local<String> writable_sym = String::NewSymbol("writable");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) & !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(proc_sym) && 
                              !args[0]->ToObject()->Get(proc_sym)->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && 
                              !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(proc_sym) && 
                              !args[0]->ToObject()->Get(proc_sym)->IsFunction()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(writable_sym) && 
                              !args[0]->ToObject()->Get(writable_sym)->IsBoolean()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_file_processor_cmn_req_t *req = 
    reinterpret_cast<kc_file_processor_cmn_req_t*>(malloc(sizeof(kc_file_processor_cmn_req_t)));
  req->type = KC_OCCUPY;
  req->result = PolyDB::Error::SUCCESS;
  req->wrapdb = obj;
  req->flag = false;
  req->proc.Clear();
  req->cb.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(writable_sym)) {
        req->flag = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
      }
      if (args[0]->ToObject()->Has(proc_sym)) {
        req->proc = Persistent<Function>::New(Handle<Function>::Cast(args[0]->ToObject()->Get(proc_sym)));
      }
    }
  } else {
    if (args[0]->ToObject()->Has(writable_sym)) {
      req->flag = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
    }
    if (args[0]->ToObject()->Has(proc_sym)) {
      req->proc = Persistent<Function>::New(Handle<Function>::Cast(args[0]->ToObject()->Get(proc_sym)));
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  SendAsyncRequest(req);

  obj->Ref();
  return scope.Close(args.This());
}


void PolyDBWrap::OnWork(uv_work_t *work_req) {
  TRACE("argument: work_req=%p\n", work_req);

  kc_req_t *req = reinterpret_cast<kc_req_t*>(work_req->data);
  PolyDBWrap *wrapdb = req->wrapdb;
  assert(wrapdb != NULL);
  PolyDB *db = wrapdb->db_;
  assert(db != NULL);

  // do operation
  switch (req->type) {
    case KC_OPEN:
      {
        kc_open_req_t *open_req = reinterpret_cast<kc_open_req_t*>(work_req->data);
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
        kc_char_cmn_req_t *remove_req = reinterpret_cast<kc_char_cmn_req_t*>(work_req->data);
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
        kc_increment_req_t *increment_req = reinterpret_cast<kc_increment_req_t*>(work_req->data);
        TRACE("increment: key = %s, num = %lld, orig = %lld\n", increment_req->key, increment_req->num, increment_req->orig);
        if (increment_req->key == NULL) {
          increment_req->result = PolyDB::Error::INVALID;
        } else {
          int64_t num = db->increment(increment_req->key, strlen(increment_req->key), increment_req->num, increment_req->orig);
          TRACE("db->increment: num = %lld\n", num);
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
        kc_increment_double_req_t *inc_dbl_req = reinterpret_cast<kc_increment_double_req_t*>(work_req->data);
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
        kc_cas_req_t *cas_req = reinterpret_cast<kc_cas_req_t*>(work_req->data);
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
    case KC_PATH:
      {
        kc_char_ret_req_t *char_ret_req = reinterpret_cast<kc_char_ret_req_t*>(work_req->data);
        const std::string &path = db->path();
        TRACE("path = %s\n", path.c_str());
        if (path.size() > 0) {
          char_ret_req->ret = kc::strdup(path.c_str());
        } else {
          char_ret_req->result = db->error().code();
        }
        break;
      }
    case KC_STATUS:
      {
        kc_strmap_ret_req_t *strmap_ret_req = reinterpret_cast<kc_strmap_ret_req_t*>(work_req->data);
        strmap_ret_req->ret = new StringMap();
        if (!db->status(strmap_ret_req->ret)) {
          strmap_ret_req->result = db->error().code();
        }
        break;
      }
    case KC_CHECK:
      {
        kc_check_req_t *check_req = reinterpret_cast<kc_check_req_t*>(work_req->data);
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
        kc_get_bulk_req_t *gb_req = reinterpret_cast<kc_get_bulk_req_t*>(work_req->data);
        if (gb_req->keys == NULL) {
          gb_req->result = PolyDB::Error::INVALID;
        } else {
          gb_req->recs = new StringMap();
          int64_t cnt = db->get_bulk(*gb_req->keys, gb_req->recs, gb_req->atomic);
          if (cnt <= 0) {
            gb_req->result = db->error().code();
          }
          TRACE("get_bulk: ret = %lld, error.code = %d\n", cnt, gb_req->result);
        }
        break;
      }
    case KC_SET_BULK:
      {
        kc_set_bulk_req_t *sb_req = reinterpret_cast<kc_set_bulk_req_t*>(work_req->data);
        if (sb_req->recs == NULL) {
          sb_req->result = PolyDB::Error::INVALID;
        } else {
          int64_t num = db->set_bulk(*sb_req->recs, sb_req->atomic);
          sb_req->num = num;
          TRACE("set_bulk: ret = %lld\n", num);
          if (num <= 0) {
            sb_req->result = db->error().code();
          }
        }
        break;
      }
    case KC_REMOVE_BULK:
      {
        kc_remove_bulk_req_t *rb_req = reinterpret_cast<kc_remove_bulk_req_t*>(work_req->data);
        if (rb_req->keys == NULL) {
          rb_req->result = PolyDB::Error::INVALID;
        } else {
          int64_t num = db->remove_bulk(*rb_req->keys, rb_req->atomic);
          rb_req->num = num;
          if (num <= 0) {
            rb_req->result = db->error().code();
          }
          TRACE("remove_bulk: ret = %lld\n", num);
        }
        break;
      }
    case KC_MATCH_PREFIX:
      { DO_MATCH_CMN_EXECUTE(match_prefix, work_req); break; }
    case KC_MATCH_REGEX:
      { DO_MATCH_CMN_EXECUTE(match_regex, work_req); break; }
    case KC_MATCH_SIMILAR:
      {
        kc_match_similar_req_t *ms_req = reinterpret_cast<kc_match_similar_req_t*>(work_req->data);
        if (ms_req->str == NULL) {
          ms_req->result = PolyDB::Error::INVALID;
        } else {
          ms_req->keys = new StringVector();
          int64_t num = db->match_similar(std::string(ms_req->str, strlen(ms_req->str)), ms_req->range, ms_req->utf, ms_req->keys, ms_req->max);
          TRACE("match_similar: ret = %lld\n", num);
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
        kc_merge_req_t *merge_req = reinterpret_cast<kc_merge_req_t*>(work_req->data);
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
    case KC_ACCEPT:
      {
        kc_accept_req_t *accept_req = reinterpret_cast<kc_accept_req_t*>(work_req->data);
        TRACE("accept: key = %s, visitor = %p, writable = %d\n", accept_req->key, &accept_req->visitor, accept_req->writable);
        if (accept_req->key == NULL || accept_req->visitor.IsEmpty()) {
          req->result = PolyDB::Error::INVALID;
        } else {
          AsyncVisitor visitor(accept_req->visitor, accept_req->writable);
          if (!db->accept(accept_req->key, strlen(accept_req->key), &visitor, accept_req->writable)) {
            req->result = db->error().code();
          }
        }
        break;
      }
    case KC_ACCEPT_BULK:
      {
        kc_accept_bulk_req_t *accept_req = reinterpret_cast<kc_accept_bulk_req_t*>(work_req->data);
        TRACE("accept_bulk : keys = %p, visitor = %p, writable = %d\n", accept_req->keys, &accept_req->visitor, accept_req->writable);
        if (accept_req->keys == NULL || accept_req->visitor.IsEmpty()) {
          req->result = PolyDB::Error::INVALID;
        } else {
          AsyncVisitor visitor(accept_req->visitor, accept_req->writable);
          if (!db->accept_bulk(*accept_req->keys, &visitor, accept_req->writable)) {
            req->result = db->error().code();
          }
        }
        break;
      }
    case KC_ITERATE:
      {
        kc_accept_req_t *ite_req = reinterpret_cast<kc_accept_req_t*>(work_req->data);
        TRACE("iterate: visitor = %p, writable = %d\n", &ite_req->visitor, ite_req->writable);
        if (ite_req->visitor.IsEmpty()) {
          req->result = PolyDB::Error::INVALID;
        } else {
          AsyncVisitor visitor(ite_req->visitor, ite_req->writable);
          if (!db->iterate(&visitor, ite_req->writable)) {
            req->result = db->error().code();
          }
        }
        break;
      }
      /*
    case KC_BEGIN_TRANSACTION:
      { DO_BOOL_PARAM_CMN_EXECUTE(begin_transaction, work_req); break; }
    case KC_END_TRANSACTION:
      { DO_BOOL_PARAM_CMN_EXECUTE(end_transaction, work_req); break; }
      */
    case KC_SYNCHRONIZE:
      {
        kc_file_processor_cmn_req_t *fproc_req = reinterpret_cast<kc_file_processor_cmn_req_t*>(work_req->data);
        TRACE("synchronize: hard = %d\n", fproc_req->flag);
        if (fproc_req->proc.IsEmpty()) {
          if (!db->synchronize(fproc_req->flag, NULL)) {
            req->result = db->error().code();
          }
        } else {
          AsyncFileProcessor fproc(fproc_req->proc);
          if (!db->synchronize(fproc_req->flag, &fproc)) {
            req->result = db->error().code();
          }
        }
        break;
      }
    case KC_OCCUPY:
      {
        kc_file_processor_cmn_req_t *fproc_req = reinterpret_cast<kc_file_processor_cmn_req_t*>(work_req->data);
        TRACE("occupy: writable = %d\n", fproc_req->flag);
        if (fproc_req->proc.IsEmpty()) {
          if (!db->occupy(fproc_req->flag, NULL)) {
            req->result = db->error().code();
          }
        } else {
          AsyncFileProcessor fproc(fproc_req->proc);
          if (!db->occupy(fproc_req->flag, &fproc)) {
            req->result = db->error().code();
          }
        }
        break;
      }
    default:
      assert(0);
  }
}


void PolyDBWrap::OnWorkDone(uv_work_t *work_req,int status) {
  TRACE("argument: work_req=%p, status %d\n", work_req,status);
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
    DEFINE_JS_CONSTANT(obj, "code", req->result);
    argv[argc] = err;
  }
  argc++;

  // other data to callback argument.
  switch (req->type) {
    case KC_GET:
    case KC_SEIZE:
      {
        kc_kv_req_t *kv_req = reinterpret_cast<kc_kv_req_t*>(work_req->data);
        if (kv_req->value) {
          argv[argc++] = String::New(kv_req->value, strlen(kv_req->value));
        }
        break;
      }
    case KC_INCREMENT:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_increment_req_t *inc_req = reinterpret_cast<kc_increment_req_t*>(work_req->data);
          TRACE("increment->num = %lld\n", inc_req->num);
          argv[argc++] = Integer::New(inc_req->num);
        }
        break;
      }
    case KC_INCREMENT_DOUBLE:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_increment_double_req_t *inc_dbl_req = reinterpret_cast<kc_increment_double_req_t*>(work_req->data);
          TRACE("increment_double->num = %f\n", inc_dbl_req->num);
          argv[argc++] = Number::New(inc_dbl_req->num);
        }
        break;
      }
    case KC_COUNT:
    case KC_SIZE:
      {
        kc_ret_req_t *ret_req = reinterpret_cast<kc_ret_req_t*>(work_req->data);
        argv[argc++] = Number::New(ret_req->ret);
        break;
      }
    case KC_PATH:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_char_ret_req_t *char_ret_req = reinterpret_cast<kc_char_ret_req_t*>(work_req->data);
          argv[argc++] = String::New(char_ret_req->ret);
        }
        break;
      }
    case KC_STATUS:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_strmap_ret_req_t *strmap_ret_req = reinterpret_cast<kc_strmap_ret_req_t*>(work_req->data);
          argv[argc++] = Map2Obj(strmap_ret_req->ret);
        }
        break;
      }
    case KC_CHECK:
      {
        kc_check_req_t *check_req = reinterpret_cast<kc_check_req_t*>(work_req->data);
        argv[argc++] = Number::New(check_req->ret);
        break;
      }
    case KC_GET_BULK:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_get_bulk_req_t *gb_req = reinterpret_cast<kc_get_bulk_req_t*>(work_req->data);
          argv[argc++] = Map2Obj(gb_req->recs);
        }
        break;
      }
    case KC_SET_BULK:
      {
        kc_set_bulk_req_t *sb_req = reinterpret_cast<kc_set_bulk_req_t*>(work_req->data);
        argv[argc++] = Number::New(sb_req->num);
        break;
      }
    case KC_REMOVE_BULK:
      {
        kc_remove_bulk_req_t *rb_req = reinterpret_cast<kc_remove_bulk_req_t*>(work_req->data);
        argv[argc++] = Number::New(rb_req->num);
        break;
      }
    case KC_MATCH_PREFIX:
    case KC_MATCH_REGEX:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_match_cmn_req_t *m_req = reinterpret_cast<kc_match_cmn_req_t*>(work_req->data);
          argv[argc++] = Vector2Array(m_req->keys);
        }
        break;
      }
    case KC_MATCH_SIMILAR:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_match_similar_req_t *ms_req = reinterpret_cast<kc_match_similar_req_t*>(work_req->data);
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
        kc_open_req_t *open_req = reinterpret_cast<kc_open_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(open_req, path);
        free(open_req);
        break;
      }
    case KC_CLOSE:
    case KC_CLEAR:
    case KC_COUNT:
    case KC_SIZE:
      {
        kc_req_t *base_req = reinterpret_cast<kc_req_t*>(work_req->data);
        free(base_req);
        break;
      }
    case KC_STATUS:
      {
        kc_strmap_ret_req_t *strmap_ret_req = reinterpret_cast<kc_strmap_ret_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(strmap_ret_req, ret);
        free(strmap_ret_req);
        break;
      }
    case KC_PATH:
      { 
        kc_char_ret_req_t *ret_req = reinterpret_cast<kc_char_ret_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(ret_req, ret);
        free(ret_req);
        break;
      }
    case KC_SET:
    case KC_GET:
    case KC_ADD:
    case KC_APPEND:
    case KC_REPLACE:
    case KC_SEIZE:
      {
        kc_kv_req_t *kv_req = reinterpret_cast<kc_kv_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(kv_req, key);
        SAFE_REQ_ATTR_FREE(kv_req, value);
        free(kv_req);
        break;
      }
    case KC_INCREMENT:
      { 
        kc_increment_req_t *inc_req = reinterpret_cast<kc_increment_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(inc_req, key);
        free(inc_req);
        break;
      }
    case KC_INCREMENT_DOUBLE:
      { 
        kc_increment_double_req_t *inc_dbl_req = reinterpret_cast<kc_increment_double_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(inc_dbl_req, key);
        free(inc_dbl_req);
        break;
      }
    case KC_CAS:
      {
        kc_cas_req_t *cas_req = reinterpret_cast<kc_cas_req_t *>(work_req->data);
        SAFE_REQ_ATTR_FREE(cas_req, key);
        SAFE_REQ_ATTR_FREE(cas_req, oval);
        SAFE_REQ_ATTR_FREE(cas_req, nval);
        free(cas_req);
        break;
      }
    case KC_CHECK:
      { 
        kc_check_req_t *check_req = reinterpret_cast<kc_check_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(check_req, key);
        free(check_req);
        break;
      }
    case KC_GET_BULK:
      {
        kc_get_bulk_req_t *gb_req = reinterpret_cast<kc_get_bulk_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(gb_req, keys);
        SAFE_REQ_ATTR_DELETE(gb_req, recs);
        free(gb_req);
        break;
      }
    case KC_SET_BULK:
      {
        kc_set_bulk_req_t *sb_req = reinterpret_cast<kc_set_bulk_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(sb_req, recs);
        free(sb_req);
        break;
      }
    case KC_REMOVE_BULK:
      {
        kc_remove_bulk_req_t *rb_req = reinterpret_cast<kc_remove_bulk_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(rb_req, keys);
        free(rb_req);
        break;
      }
    case KC_MATCH_PREFIX:
    case KC_MATCH_REGEX:
      {
        kc_match_cmn_req_t *m_req = reinterpret_cast<kc_match_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(m_req, str);
        SAFE_REQ_ATTR_DELETE(m_req, keys);
        free(m_req);
        break;
      }
    case KC_MATCH_SIMILAR:
      {
        kc_match_similar_req_t *ms_req = reinterpret_cast<kc_match_similar_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(ms_req, str);
        SAFE_REQ_ATTR_DELETE(ms_req, keys);
        free(ms_req);
        break;
      }
    case KC_MERGE:
      {
        kc_merge_req_t *merge_req = reinterpret_cast<kc_merge_req_t*>(work_req->data);
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
        kc_char_cmn_req_t *path_req = reinterpret_cast<kc_char_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(path_req, str);
        free(path_req);
        break;
      }
    case KC_ACCEPT:
      {
        kc_accept_req_t *accept_req = reinterpret_cast<kc_accept_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(accept_req, key);
        accept_req->visitor.Dispose();
        free(accept_req);
        break;
      }
    case KC_ACCEPT_BULK:
      {
        kc_accept_bulk_req_t *accept_req = reinterpret_cast<kc_accept_bulk_req_t*>(work_req->data);
        SAFE_REQ_ATTR_DELETE(accept_req, keys);
        accept_req->visitor.Dispose();
        free(accept_req);
        break;
      }
    case KC_ITERATE:
      {
        kc_accept_req_t *ite_req = reinterpret_cast<kc_accept_req_t*>(work_req->data);
        ite_req->visitor.Dispose();
        free(ite_req);
        break;
      }
      /*
    case KC_BEGIN_TRANSACTION:
    case KC_END_TRANSACTION:
      {
        kc_boolean_cmn_req_t *tran_req = static_cast<kc_boolean_cmn_req_t*>(work_req->data);
        free(tran_req);
        break;
      }
      */
    case KC_SYNCHRONIZE:
    case KC_OCCUPY:
      {
        kc_file_processor_cmn_req_t *fproc_req = reinterpret_cast<kc_file_processor_cmn_req_t*>(work_req->data);
        fproc_req->proc.Dispose();
        free(fproc_req);
        break;
      }
    default:
      assert(0);
  }
  work_req->data = NULL;

  free(work_req);
}

void PolyDBWrap::Init(Handle<Object> target) {
  TRACE("load db module\n");

  // prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("DB"));

  Local<ObjectTemplate> insttpl = tpl->InstanceTemplate();
  insttpl->SetInternalFieldCount(1);

  // prototype
  Local<ObjectTemplate> prottpl = tpl->PrototypeTemplate();
  DEFINE_JS_METHOD(prottpl, "open", Open);
  DEFINE_JS_METHOD(prottpl, "close", Close);
  DEFINE_JS_METHOD(prottpl, "set", Set);
  DEFINE_JS_METHOD(prottpl, "get", Get);
  DEFINE_JS_METHOD(prottpl, "clear", Clear);
  DEFINE_JS_METHOD(prottpl, "add", Add);
  DEFINE_JS_METHOD(prottpl, "append", Append);
  DEFINE_JS_METHOD(prottpl, "remove", Remove);
  DEFINE_JS_METHOD(prottpl, "replace", Replace);
  DEFINE_JS_METHOD(prottpl, "seize", Seize);
  DEFINE_JS_METHOD(prottpl, "increment", Increment);
  DEFINE_JS_METHOD(prottpl, "increment_double", IncrementDouble);
  DEFINE_JS_METHOD(prottpl, "cas", Cas);
  DEFINE_JS_METHOD(prottpl, "count", Count);
  DEFINE_JS_METHOD(prottpl, "size", Size);
  DEFINE_JS_METHOD(prottpl, "path", Path);
  DEFINE_JS_METHOD(prottpl, "status", Status);
  DEFINE_JS_METHOD(prottpl, "check", Check);
  DEFINE_JS_METHOD(prottpl, "get_bulk", GetBulk);
  DEFINE_JS_METHOD(prottpl, "set_bulk", SetBulk);
  DEFINE_JS_METHOD(prottpl, "remove_bulk", RemoveBulk);
  DEFINE_JS_METHOD(prottpl, "match_prefix", MatchPrefix);
  DEFINE_JS_METHOD(prottpl, "match_regex", MatchRegex);
  DEFINE_JS_METHOD(prottpl, "match_similar", MatchSimilar);
  DEFINE_JS_METHOD(prottpl, "copy", Copy);
  DEFINE_JS_METHOD(prottpl, "merge", Merge);
  DEFINE_JS_METHOD(prottpl, "dump_snapshot", DumpSnapshot);
  DEFINE_JS_METHOD(prottpl, "load_snapshot", LoadSnapshot);
  DEFINE_JS_METHOD(prottpl, "accept", Accept);
  DEFINE_JS_METHOD(prottpl, "accept_bulk", AcceptBulk);
  DEFINE_JS_METHOD(prottpl, "iterate", Iterate);
  DEFINE_JS_METHOD(prottpl, "begin_transaction", BeginTransaction);
  DEFINE_JS_METHOD(prottpl, "end_transaction", EndTransaction);
  DEFINE_JS_METHOD(prottpl, "synchronize", Synchronize);
  DEFINE_JS_METHOD(prottpl, "occupy", Occupy);

  Persistent<Function> ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("DB"), ctor);

  // define OpenMode
  DEFINE_JS_CONSTANT(ctor, "OREADER", PolyDB::OREADER);
  DEFINE_JS_CONSTANT(ctor, "OWRITER", PolyDB::OWRITER);
  DEFINE_JS_CONSTANT(ctor, "OCREATE", PolyDB::OCREATE);
  DEFINE_JS_CONSTANT(ctor, "OTRUNCATE", PolyDB::OTRUNCATE);
  DEFINE_JS_CONSTANT(ctor, "OAUTOTRAN", PolyDB::OAUTOTRAN);
  DEFINE_JS_CONSTANT(ctor, "OAUTOSYNC", PolyDB::OAUTOSYNC);
  DEFINE_JS_CONSTANT(ctor, "ONOLOCK", PolyDB::ONOLOCK);
  DEFINE_JS_CONSTANT(ctor, "OTRYLOCK", PolyDB::OTRYLOCK);
  DEFINE_JS_CONSTANT(ctor, "ONOREPAIR", PolyDB::ONOREPAIR);

  // define MergeMode
  DEFINE_JS_CONSTANT(ctor, "MSET", PolyDB::MSET);
  DEFINE_JS_CONSTANT(ctor, "MADD", PolyDB::MADD);
  DEFINE_JS_CONSTANT(ctor, "MREPLACE", PolyDB::MREPLACE);
  DEFINE_JS_CONSTANT(ctor, "MAPPEND", PolyDB::MAPPEND);

  // define MapReduceMode
  DEFINE_JS_CONSTANT(ctor, "XNOLOCK", MapReduce::XNOLOCK);
  DEFINE_JS_CONSTANT(ctor, "XPARAMAP", MapReduce::XPARAMAP);
  DEFINE_JS_CONSTANT(ctor, "XPARARED", MapReduce::XPARARED);
  DEFINE_JS_CONSTANT(ctor, "XPARAFLS", MapReduce::XPARAFLS);
  DEFINE_JS_CONSTANT(ctor, "XNOCOMP",  MapReduce::XNOCOMP);
}

