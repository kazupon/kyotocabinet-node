/*
 * polydb wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include "polydb_wrap.h"
#include <assert.h>
#include "debug.h"
#include <kcdbext.h>

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

#define DO_RET_EXECUTE(Method, work_req)                            \
  kc_ret_req_t *req = static_cast<kc_ret_req_t *>(work_req->data);  \
  req->ret = db->Method();                                          \
  if (req->ret == -1) {                                             \
    req->result = db->error().code();                               \
  }                                                                 \

#define DO_KV_V_RET_EXECUTE(Method, work_req)                           \
  kc_kv_req_t *req = static_cast<kc_kv_req_t *>(work_req->data);        \
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

#define DO_KV_EXECUTE(Method, work_req)                           \
  kc_kv_req_t *req = static_cast<kc_kv_req_t *>(work_req->data);  \
  TRACE("key = %s, value = %s\n", req->key, req->value);          \
  if (req->key == NULL || req->value == NULL) {                   \
    req->result = PolyDB::Error::INVALID;                         \
  } else {                                                        \
    if (!db->Method(req->key, strlen(req->key),                   \
                    req->value, strlen(req->value))) {            \
      req->result = db->error().code();                           \
    }                                                             \
  }                                                               \

#define KV_FREE(work_req)                                         \
  kc_kv_req_t *req = static_cast<kc_kv_req_t *>(work_req->data);  \
  if (req->key != NULL) {                                         \
    free(req->key);                                               \
    req->key = NULL;                                              \
  }                                                               \
  if (req->value != NULL) {                                       \
    free(req->value);                                             \
    req->value = NULL;                                            \
  }                                                               \
  free(req);                                                      \

#define K_FREE(work_req, REQ_TYPE)                          \
  REQ_TYPE *req = static_cast<REQ_TYPE *>(work_req->data);  \
  if (req->key != NULL) {                                   \
    free(req->key);                                         \
    req->key = NULL;                                        \
  }                                                         \
  free(req);                                                \


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
};

// common request field
#define KC_REQ_FIELD            \
  PolyDBWrap *wrapdb;           \
  kc_req_type type;             \
  PolyDB::Error::Code result;   \
  Persistent<Function> cb;      \

typedef std::map<std::string, std::string> StringMap;

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

// remove request
typedef struct kc_remove_req_t {
  KC_REQ_FIELD
  char *key;
} kc_remove_req_t;

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


PolyDBWrap::PolyDBWrap() {
  TRACE("constructor\n");
  db_ = new PolyDB();
  assert(db_ != NULL);
}

PolyDBWrap::~PolyDBWrap() {
  TRACE("destructor\n");
  assert(db_ != NULL);
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

  kc_remove_req_t *remove_req = (kc_remove_req_t *)malloc(sizeof(kc_remove_req_t));
  remove_req->type = KC_REMOVE;
  remove_req->result = PolyDB::Error::SUCCESS;
  remove_req->wrapdb = obj;
  remove_req->key = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      remove_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        remove_req->key = kc::strdup(*key);
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      remove_req->key = kc::strdup(*key);
    }
    remove_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = remove_req;

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
        kc_remove_req_t *remove_req = static_cast<kc_remove_req_t *>(work_req->data);
        TRACE("remove: key = %s\n", remove_req->key);
        if (remove_req->key == NULL) {
          req->result = PolyDB::Error::INVALID;
        } else {
          if (!db->remove(remove_req->key, strlen(remove_req->key))) {
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
          Local<Object> ret = Object::New();
          StringMap::const_iterator it = strmap_ret_req->ret->begin();
          StringMap::const_iterator it_end = strmap_ret_req->ret->end();
          while (it != it_end) {
            Local<String> key = String::New(it->first.c_str(), it->first.length());
            Local<String> val = String::New(it->second.c_str(), it->second.length());
            ret->Set(key, val);
            ++it;
          }
          argv[argc++] = ret;
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
        kc_open_req_t *open_req = static_cast<kc_open_req_t *>(work_req->data);
        assert(open_req->path != NULL);
        free(open_req->path);
        open_req->path = NULL;
        free(open_req);
        break;
      }
    case KC_CLOSE:
    case KC_CLEAR:
    case KC_COUNT:
    case KC_SIZE:
      free(req);
      break;
    case KC_STATUS:
      {
        kc_strmap_ret_req_t *strmap_ret_req = static_cast<kc_strmap_ret_req_t*>(work_req->data);
        if (strmap_ret_req->ret) {
          delete strmap_ret_req->ret;
          strmap_ret_req->ret = NULL;
        }
        free(req);
        break;
      }
    case KC_SET:
    case KC_GET:
    case KC_ADD:
    case KC_APPEND:
    case KC_REPLACE:
    case KC_SEIZE:
      { KV_FREE(work_req); break; }
    case KC_REMOVE:
      { K_FREE(work_req, kc_remove_req_t); break; }
    case KC_INCREMENT:
      { K_FREE(work_req, kc_increment_req_t); break; }
    case KC_INCREMENT_DOUBLE:
      { K_FREE(work_req, kc_increment_double_req_t); break; }
    case KC_CAS:
      {
        kc_cas_req_t *cas_req = static_cast<kc_cas_req_t *>(work_req->data);
        if (cas_req->key) {
          free(cas_req->key);
          cas_req->key = NULL;
        }
        if (cas_req->oval) {
          free(cas_req->oval);
          cas_req->oval = NULL;
        }
        if (cas_req->nval) {
          free(cas_req->nval);
          cas_req->nval = NULL;
        }
        free(req);
        break;
      }
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

