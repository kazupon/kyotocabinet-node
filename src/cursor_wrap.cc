/*
 * cursor wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include "cursor_wrap.h" 
#include "polydb_wrap.h"
#include "debug.h"
#include "utils.h"
#include <assert.h>

using namespace v8;
using namespace kyotocabinet;
namespace kc = kyotocabinet;


// request type
enum kc_cur_req_type {
  KC_CUR_CREATE,
  KC_CUR_JUMP,
  KC_CUR_JUMP_BACK,
  KC_CUR_STEP,
  KC_CUR_STEP_BACK,
  KC_CUR_GET,
};

// common request field
#define KC_CUR_REQ_FIELD        \
  CursorWrap *wrapcur;          \
  kc_cur_req_type type;         \
  PolyDB::Error::Code result;   \
  Persistent<Function> cb;      \

// cursor base request
typedef struct kc_cur_req_t {
  KC_CUR_REQ_FIELD
} kc_cur_req_type_t;

// cursor create request
typedef struct kc_cur_create_req_t {
  KC_CUR_REQ_FIELD
  PolyDBWrap *wrapdb;
  CursorWrap *retcur;
  Persistent<Object> self;
} kc_cur_create_req_t;

// cursor common request
typedef struct kc_cur_cmn_req_t {
  KC_CUR_REQ_FIELD
  char *key;
  char *value;
  bool step;
  bool writable;
} kc_cur_cmn_req_t;


CursorWrap::CursorWrap(PolyDB::Cursor *cursor) : cursor_(cursor) {
  TRACE("ctor: cursor_ = %p\n", cursor_);
  assert(cursor_ != NULL);
  wrapdb_ = NULL;
}

CursorWrap::~CursorWrap() {
  TRACE("destor: wrapdb_ = %p, cursor_ = %p\n", wrapdb_, cursor_);
  if (wrapdb_) {
    wrapdb_->Unref();
  }
  if (cursor_) {
    delete cursor_;
    cursor_ = NULL;
  }
  wrapdb_ = NULL;
}

void CursorWrap::SetWrapDB(PolyDBWrap *wrapdb) {
  wrapdb_ = wrapdb;
  TRACE("wrapdb_ = %p\n", wrapdb_);
  wrapdb_->Ref();
}

PolyDB::Error::Code CursorWrap::GetErrorCode() {
  assert(wrapdb_ != NULL);
  return wrapdb_->db_->error().code();
}


Persistent<Function> CursorWrap::ctor;


Handle<Value> CursorWrap::New(const Arguments &args) {
  HandleScope scope;
  TRACE("New\n");

  if ((args.Length() == 0) || (args.Length() == 1 && args[0]->IsFunction())) {
    ThrowException(Exception::Error(String::New("Invalid parameter")));
    return args.This();
  }

  Local<String> ctor_sym = String::NewSymbol("constructor");
  Local<String> name_sym = String::NewSymbol("name");
  String::Utf8Value ctorName(args[0]->ToObject()->Get(ctor_sym)->ToObject()->Get(name_sym)->ToString());
  if (strcmp("DB", *ctorName)) {
    ThrowException(Exception::Error(String::New("Invalid parameter")));
    return args.This();
  }

  if (args.Length() == 1 && !args[0]->IsFunction()) {
    PolyDBWrap *dbWrap = ObjectWrap::Unwrap<PolyDBWrap>(args[0]->ToObject());
    CursorWrap *cursorWrap = new CursorWrap(dbWrap->Cursor());
    cursorWrap->Wrap(args.This());
  } else {
    kc_cur_create_req_t *req = (kc_cur_create_req_t *)malloc(sizeof(kc_cur_create_req_t));
    req->type = KC_CUR_CREATE;
    req->wrapcur = NULL;
    req->result = PolyDB::Error::SUCCESS;
    req->wrapdb = ObjectWrap::Unwrap<PolyDBWrap>(args[0]->ToObject());
    req->wrapdb->Ref();
    req->retcur = NULL;
    req->self.Clear();
    req->cb.Clear();

    req->self = Persistent<Object>::New(Handle<Object>::Cast(args.This()));
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));

    uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
    uv_req->data = req;

    int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
    TRACE("uv_queue_work: ret=%d\n", ret);
  }

  return args.This();
}

Handle<Value> CursorWrap::Create(const Arguments &args) {
  HandleScope scope;
  TRACE("Create\n");

  if (args.Length() > 2) {
    ThrowException(Exception::Error(String::New("Invalid parameter")));
    return scope.Close(Undefined());
  }

  Local<Value> argv[2] = { 
    Local<Value>::New(Null()),
    Local<Value>::New(Null()),
  };
  for (int i = 0; i < args.Length(); i++) {
    argv[i] = args[i];
  }

  return scope.Close(ctor->NewInstance(args.Length(), argv));
}

Handle<Value> CursorWrap::Jump(const Arguments &args) {
  HandleScope scope;
  TRACE("Jump\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsString() & !args[0]->IsFunction())) ||
       (args.Length() == 2 && (!args[0]->IsString() | !args[1]->IsFunction())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
  req->type = KC_CUR_JUMP;
  req->wrapcur = wrapCur;
  req->result = PolyDB::Error::SUCCESS;
  req->key = NULL;
  req->value = NULL;
  req->step = false;
  req->writable = false;
  req->cb.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else {
      String::Utf8Value key(args[0]->ToString());
      req->key = kc::strdup(*key);
    }
  } else {
    String::Utf8Value key(args[0]->ToString());
    req->key = kc::strdup(*key);
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  wrapCur->Ref();

  return args.This();
}

Handle<Value> CursorWrap::JumpBack(const Arguments &args) {
  HandleScope scope;
  TRACE("JumpBack\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsString() & !args[0]->IsFunction())) ||
       (args.Length() == 2 && (!args[0]->IsString() | !args[1]->IsFunction())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
  req->type = KC_CUR_JUMP_BACK;
  req->wrapcur = wrapCur;
  req->result = PolyDB::Error::SUCCESS;
  req->key = NULL;
  req->value = NULL;
  req->step = false;
  req->writable = false;
  req->cb.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else {
      String::Utf8Value key(args[0]->ToString());
      req->key = kc::strdup(*key);
    }
  } else {
    String::Utf8Value key(args[0]->ToString());
    req->key = kc::strdup(*key);
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  wrapCur->Ref();

  return args.This();
}

Handle<Value> CursorWrap::Step(const Arguments &args) {
  HandleScope scope;
  TRACE("Step\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && !args[0]->IsFunction()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
  req->type = KC_CUR_STEP;
  req->wrapcur = wrapCur;
  req->result = PolyDB::Error::SUCCESS;
  req->key = NULL;
  req->value = NULL;
  req->step = false;
  req->writable = false;
  req->cb.Clear();

  req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));

  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  wrapCur->Ref();

  return args.This();
}

Handle<Value> CursorWrap::StepBack(const Arguments &args) {
  HandleScope scope;
  TRACE("StepBack\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && !args[0]->IsFunction()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
  req->type = KC_CUR_STEP_BACK;
  req->wrapcur = wrapCur;
  req->result = PolyDB::Error::SUCCESS;
  req->key = NULL;
  req->value = NULL;
  req->step = false;
  req->writable = false;
  req->cb.Clear();

  req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));

  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  wrapCur->Ref();

  return args.This();
}

Handle<Value> CursorWrap::Get(const Arguments &args) {
  HandleScope scope;
  TRACE("Get\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsBoolean() & !args[0]->IsFunction())) ||
       (args.Length() == 2 && (!args[0]->IsBoolean() | !args[1]->IsFunction())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return args.This();
  }

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
  req->type = KC_CUR_GET;
  req->wrapcur = wrapCur;
  req->result = PolyDB::Error::SUCCESS;
  req->key = NULL;
  req->value = NULL;
  req->step = false;
  req->writable = false;
  req->cb.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else {
      req->step = args[0]->BooleanValue();
    }
  } else {
    req->step = args[0]->BooleanValue();
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);

  wrapCur->Ref();

  return args.This();
}


void CursorWrap::OnWork(uv_work_t *work_req) {
  TRACE("argument: work_req=%p\n", work_req);

  kc_cur_req_type_t *req = static_cast<kc_cur_req_type_t*>(work_req->data);
  assert(req != NULL);

  // do operation
  switch (req->type) {
    case KC_CUR_CREATE:
      {
        kc_cur_create_req_t *cur_req = static_cast<kc_cur_create_req_t*>(work_req->data);
        cur_req->retcur = new CursorWrap(cur_req->wrapdb->Cursor());
        break;
      }
    case KC_CUR_JUMP:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (cur_req->key == NULL) {
          if (!wrapCur->cursor_->jump()) {
            cur_req->result = wrapCur->GetErrorCode();
          }
        } else {
          if (!wrapCur->cursor_->jump(cur_req->key, strlen(cur_req->key))) {
            cur_req->result = wrapCur->GetErrorCode();
          }
        }
        break;
      }
    case KC_CUR_JUMP_BACK:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (cur_req->key == NULL) {
          if (!wrapCur->cursor_->jump_back()) {
            cur_req->result = wrapCur->GetErrorCode();
          }
        } else {
          if (!wrapCur->cursor_->jump_back(cur_req->key, strlen(cur_req->key))) {
            cur_req->result = wrapCur->GetErrorCode();
          }
        }
        break;
      }
    case KC_CUR_STEP:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (!wrapCur->cursor_->step()) {
          cur_req->result = wrapCur->GetErrorCode();
        }
        break;
      }
    case KC_CUR_STEP_BACK:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (!wrapCur->cursor_->step_back()) {
          cur_req->result = wrapCur->GetErrorCode();
        }
        break;
      }
    case KC_CUR_GET:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        TRACE("cursor get: step = %d\n", cur_req->step);
        const char *value = NULL;
        size_t key_size, value_size;
        char *key = wrapCur->cursor_->get(&key_size, &value, &value_size, cur_req->step);
        TRACE("cursor->get: key = %s(%p), value = %s(%p)\n", key, key, value, value);
        if (key == NULL || value == NULL) {
          cur_req->result = wrapCur->GetErrorCode();
        } else {
          cur_req->key = key;
          cur_req->value = kc::strdup((char *)value);
        }
        break;
      }
    default:
      assert(0);
      break;
  }
}


void CursorWrap::OnWorkDone(uv_work_t *work_req) {
  HandleScope scope;
  TRACE("argument: work_req=%p\n", work_req);

  kc_cur_req_type_t *req = static_cast<kc_cur_req_type_t*>(work_req->data);
  assert(req != NULL);

  // init callback arguments.
  int argc = 0;
  Local<Value> argv[3] = { 
    Local<Value>::New(Null()),
    Local<Value>::New(Null()),
    Local<Value>::New(Null()),
  };

  // set error to callback arguments.
  if (req->type != KC_CUR_CREATE) {
    if (req->result != PolyDB::Error::SUCCESS) {
      const char *name = PolyDB::Error::codename(req->result);
      Local<String> message = String::NewSymbol(name);
      Local<Value> err = Exception::Error(message);
      Local<Object> obj = err->ToObject();
      obj->Set(String::NewSymbol("code"), Integer::New(req->result), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
      argv[argc] = err;
    }
    argc++;
  }

  // other data to callback argument.
  switch (req->type) {
    case KC_CUR_CREATE:
      {
        kc_cur_create_req_t *cur_req = static_cast<kc_cur_create_req_t*>(work_req->data);
        if (cur_req->retcur == NULL) {
          Local<String> message = String::NewSymbol("Cannot create object");
          argv[argc] = Exception::Error(message);
        }
        argc++;
        if (cur_req->retcur) {
          cur_req->retcur->Wrap(cur_req->self);
          cur_req->retcur->SetWrapDB(cur_req->wrapdb);
          argv[argc++] = cur_req->self->ToObject();
        }
        break;
      }
    case KC_CUR_GET:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
          argv[argc++] = String::New(cur_req->key, strlen(cur_req->key));
          argv[argc++] = String::New(cur_req->value, strlen(cur_req->value));
        }
        break;
      }
    default:
      break;
  }


  //
  // execute callback
  //

  if (!req->cb.IsEmpty()) {
    TryCatch try_catch;
    if (req->type == KC_CUR_CREATE) {
      req->cb->Call(Context::GetCurrent()->Global(), argc, argv);
    } else {
      MakeCallback(req->wrapcur->handle_, req->cb, argc, argv);
    }
    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }
  } 


  //
  // releases
  //

  if (req->wrapcur) {
    req->wrapcur->Unref();
  }
  req->wrapcur = NULL;
  req->cb.Dispose();

  switch (req->type) {
    case KC_CUR_CREATE:
      {
        kc_cur_create_req_t *cur_req = static_cast<kc_cur_create_req_t*>(work_req->data);
        cur_req->wrapdb->Unref();
        cur_req->wrapdb = NULL;
        cur_req->self.Dispose();
        cur_req->retcur = NULL;
        free(cur_req);
        break;
      }
    case KC_CUR_JUMP:
    case KC_CUR_JUMP_BACK:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(cur_req, key);
        free(cur_req);
        break;
      }
    case KC_CUR_STEP:
    case KC_CUR_STEP_BACK:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        free(cur_req);
        break;
      }
    case KC_CUR_GET:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(cur_req, key);
        SAFE_REQ_ATTR_FREE(cur_req, value);
        free(cur_req);
        break;
      }
    default:
      assert(0);
  }
  work_req->data = NULL;

  free(work_req);
}


void CursorWrap::Init(Handle<Object> target) {
  TRACE("load cursor module\n");
   
  // prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("Cursor"));

  Local<ObjectTemplate> insttpl = tpl->InstanceTemplate();
  insttpl->SetInternalFieldCount(1);

  // prototype(s)
  Local<ObjectTemplate> prottpl = tpl->PrototypeTemplate();
  prottpl->Set(String::NewSymbol("jump"), FunctionTemplate::New(Jump)->GetFunction());
  prottpl->Set(String::NewSymbol("jump_back"), FunctionTemplate::New(JumpBack)->GetFunction());
  prottpl->Set(String::NewSymbol("step"), FunctionTemplate::New(Step)->GetFunction());
  prottpl->Set(String::NewSymbol("step_back"), FunctionTemplate::New(StepBack)->GetFunction());
  prottpl->Set(String::NewSymbol("get"), FunctionTemplate::New(Get)->GetFunction());

  ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("Cursor"), ctor);

  // define function(s)
  ctor->Set(String::NewSymbol("create"), FunctionTemplate::New(Create)->GetFunction());
}

