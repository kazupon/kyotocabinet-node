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
};

// common request field
#define KC_REQ_FIELD            \
  PolyDBWrap *wrapdb;           \
  kc_req_type type;             \
  PolyDB::Error::Code result;   \
  Persistent<Function> cb;      \

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

// set/get request
typedef struct kc_set_req_t {
  KC_REQ_FIELD
  char *key;
  char *value;
} kc_set_req_t, kc_get_req_t, kc_add_req_t, kc_append_req_t;

// remove request
typedef struct kc_remove_req_t {
  KC_REQ_FIELD
  char *key;
} kc_remove_req_t;


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

Handle<Value> PolyDBWrap::Close(const Arguments &args) {
  TRACE("Close\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  kc_req_t *close_req = (kc_req_t *)malloc(sizeof(kc_req_t));
  close_req->type = KC_CLOSE;
  close_req->result = PolyDB::Error::SUCCESS;
  close_req->wrapdb = obj;

  if (args.Length() > 0 && args[0]->IsFunction()) {
    close_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = close_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::Set(const Arguments &args) {
  TRACE("Set\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  Local<String> value_sym = String::NewSymbol("value");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(value_sym) && !args[0]->ToObject()->Get(value_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(value_sym) && !args[0]->ToObject()->Get(value_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_set_req_t *set_req = (kc_set_req_t *)malloc(sizeof(kc_set_req_t));
  set_req->type = KC_SET;
  set_req->result = PolyDB::Error::SUCCESS;
  set_req->wrapdb = obj;
  set_req->key = NULL;
  set_req->value = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      set_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        set_req->key = kc::strdup(*key);
      }
      if (args[0]->ToObject()->Has(value_sym)) {
        String::Utf8Value value(args[0]->ToObject()->Get(value_sym));
        set_req->value = kc::strdup(*value);
      }
    }
  } else {
    assert(args[0]->IsObject() && args[1]->IsFunction());
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      set_req->key = kc::strdup(*key);
    }
    if (args[0]->ToObject()->Has(value_sym)) {
      String::Utf8Value value(args[0]->ToObject()->Get(value_sym));
      set_req->value = kc::strdup(*value);
    }
    set_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = set_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::Get(const Arguments &args) {
  TRACE("Get\n");
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

  kc_get_req_t *get_req = (kc_get_req_t *)malloc(sizeof(kc_get_req_t));
  get_req->type = KC_GET;
  get_req->result = PolyDB::Error::SUCCESS;
  get_req->wrapdb = obj;
  get_req->key = NULL;
  get_req->value = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      get_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        get_req->key = kc::strdup(*key);
      }
    }
  } else {
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      get_req->key = kc::strdup(*key);
    }
    get_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = get_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::Clear(const Arguments &args) {
  TRACE("Clear\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  kc_req_t *clear_req = (kc_req_t *)malloc(sizeof(kc_req_t));
  clear_req->type = KC_CLEAR;
  clear_req->result = PolyDB::Error::SUCCESS;
  clear_req->wrapdb = obj;

  if (args.Length() > 0 && args[0]->IsFunction()) {
    clear_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = clear_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::Add(const Arguments &args) {
  TRACE("Add\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  Local<String> value_sym = String::NewSymbol("value");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(value_sym) && !args[0]->ToObject()->Get(value_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(value_sym) && !args[0]->ToObject()->Get(value_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_add_req_t *add_req = (kc_add_req_t *)malloc(sizeof(kc_add_req_t));
  add_req->type = KC_ADD;
  add_req->result = PolyDB::Error::SUCCESS;
  add_req->wrapdb = obj;
  add_req->key = NULL;
  add_req->value = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      add_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        add_req->key = kc::strdup(*key);
      }
      if (args[0]->ToObject()->Has(value_sym)) {
        String::Utf8Value value(args[0]->ToObject()->Get(value_sym));
        add_req->value = kc::strdup(*value);
      }
    }
  } else {
    assert(args[0]->IsObject() && args[1]->IsFunction());
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      add_req->key = kc::strdup(*key);
    }
    if (args[0]->ToObject()->Has(value_sym)) {
      String::Utf8Value value(args[0]->ToObject()->Get(value_sym));
      add_req->value = kc::strdup(*value);
    }
    add_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = add_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

Handle<Value> PolyDBWrap::Append(const Arguments &args) {
  TRACE("Append\n");
  HandleScope scope;

  PolyDBWrap *obj = ObjectWrap::Unwrap<PolyDBWrap>(args.This());
  assert(obj != NULL);

  Local<String> key_sym = String::NewSymbol("key");
  Local<String> value_sym = String::NewSymbol("value");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 1 && args[0]->IsObject() && args[0]->ToObject()->Has(value_sym) && !args[0]->ToObject()->Get(value_sym)->IsString()) ||
       (args.Length() == 2 && (!args[0]->IsObject() || !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(key_sym) && !args[0]->ToObject()->Get(key_sym)->IsString()) ||
       (args.Length() == 2 && args[0]->IsObject() && args[0]->ToObject()->Has(value_sym) && !args[0]->ToObject()->Get(value_sym)->IsString()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_append_req_t *append_req = (kc_append_req_t *)malloc(sizeof(kc_append_req_t));
  append_req->type = KC_APPEND;
  append_req->result = PolyDB::Error::SUCCESS;
  append_req->wrapdb = obj;
  append_req->key = NULL;
  append_req->value = NULL;

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      append_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else {
      assert(args[0]->IsObject());
      if (args[0]->ToObject()->Has(key_sym)) {
        String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
        append_req->key = kc::strdup(*key);
      }
      if (args[0]->ToObject()->Has(value_sym)) {
        String::Utf8Value value(args[0]->ToObject()->Get(value_sym));
        append_req->value = kc::strdup(*value);
      }
    }
  } else {
    assert(args[0]->IsObject() && args[1]->IsFunction());
    if (args[0]->ToObject()->Has(key_sym)) {
      String::Utf8Value key(args[0]->ToObject()->Get(key_sym));
      append_req->key = kc::strdup(*key);
    }
    if (args[0]->ToObject()->Has(value_sym)) {
      String::Utf8Value value(args[0]->ToObject()->Get(value_sym));
      append_req->value = kc::strdup(*value);
    }
    append_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = append_req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  obj->Ref();
  return args.This();
}

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
      if (!db->close()) {
        req->result = db->error().code();
      }
      break;
    case KC_SET:
      {
        kc_set_req_t *set_req = static_cast<kc_set_req_t *>(work_req->data);
        TRACE("set: key = %s, value = %s\n", set_req->key, set_req->value);
        if (set_req->key == NULL || set_req->value == NULL) {
          req->result = PolyDB::Error::INVALID;
        } else {
          if (!db->set(set_req->key, strlen(set_req->key), set_req->value, strlen(set_req->value))) {
            req->result = db->error().code();
          }
        }
        break;
      }
    case KC_GET:
      {
        kc_get_req_t *get_req = static_cast<kc_get_req_t *>(work_req->data);
        TRACE("get: key = %s\n", get_req->key);
        size_t value_size;
        if (get_req->key == NULL) {
          req->result = PolyDB::Error::INVALID;
        } else {
          get_req->value = db->get(get_req->key, strlen(get_req->key), &value_size);
          TRACE("get: return value = %s, size = %d\n", get_req->value, value_size);
          if (get_req->value == NULL) {
            req->result = db->error().code();
          }
        }
        break;
      }
    case KC_CLEAR:
      if (!db->clear()) {
        req->result = db->error().code();
      }
      break;
    case KC_ADD:
      {
        kc_add_req_t *add_req = static_cast<kc_add_req_t *>(work_req->data);
        TRACE("add: key = %s, value = %s\n", add_req->key, add_req->value);
        if (add_req->key == NULL || add_req->value == NULL) {
          req->result = PolyDB::Error::INVALID;
        } else {
          if(!db->add(add_req->key, strlen(add_req->key), add_req->value, strlen(add_req->value))) {
            req->result = db->error().code();
          }
        }
        break;
      }
    case KC_APPEND:
      {
        kc_append_req_t *append_req = static_cast<kc_append_req_t *>(work_req->data);
        TRACE("append: key = %s, value = %s\n", append_req->key, append_req->value);
        if (append_req->key == NULL || append_req->value == NULL) {
          req->result = PolyDB::Error::INVALID;
        } else {
          if(!db->append(append_req->key, strlen(append_req->key), append_req->value, strlen(append_req->value))) {
            req->result = db->error().code();
          }
        }
        break;
      }
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
      {
        kc_get_req_t *get_req = static_cast<kc_get_req_t *>(work_req->data);
        if (get_req->value) {
          argv[argc++] = String::New(get_req->value, strlen(get_req->value));
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
    MakeCallback(wrapdb->handle_, req->cb, argc, argv);
    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }
  } 


  // releases

  wrapdb->Unref();
  req->cb.Dispose();

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
      free(req);
      break;
    case KC_SET:
      {
        kc_set_req_t *set_req = static_cast<kc_set_req_t *>(work_req->data);
        if (set_req->key != NULL) {
          free(set_req->key);
          set_req->key = NULL;
        }
        if (set_req->value != NULL) {
          free(set_req->value);
          set_req->value = NULL;
        }
        free(set_req);
        break;
      }
    case KC_GET:
      {
        kc_get_req_t *get_req = static_cast<kc_get_req_t *>(work_req->data);
        if (get_req->key != NULL) {
          free(get_req->key);
          get_req->key = NULL;
        }
        if (get_req->value) {
          free(get_req->value);
          get_req->value = NULL;
        }
        free(get_req);
        break;
      }
    case KC_ADD:
      {
        kc_add_req_t *add_req = static_cast<kc_add_req_t *>(work_req->data);
        if (add_req->key != NULL) {
          free(add_req->key);
          add_req->key = NULL;
        }
        if (add_req->value != NULL) {
          free(add_req->value);
          add_req->value = NULL;
        }
        free(add_req);
        break;
      }
    case KC_APPEND:
      {
        kc_append_req_t *append_req = static_cast<kc_append_req_t *>(work_req->data);
        if (append_req->key != NULL) {
          free(append_req->key);
          append_req->key = NULL;
        }
        if (append_req->value != NULL) {
          free(append_req->value);
          append_req->value = NULL;
        }
        free(append_req);
        break;
      }
    case KC_REMOVE:
      {
        kc_remove_req_t *remove_req = static_cast<kc_remove_req_t *>(work_req->data);
        if (remove_req->key != NULL) {
          free(remove_req->key);
          remove_req->key = NULL;
        }
        free(remove_req);
        break;
      }
    default:
      assert(0);
  }
  req->wrapdb = NULL;
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

