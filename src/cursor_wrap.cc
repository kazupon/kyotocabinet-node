/*
 * cursor wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include "cursor_wrap.h" 
#include "async.h"
#include "polydb_wrap.h"
#include "debug.h"
#include "utils.h"
#include <assert.h>
#include <kcdb.h>
#include <pthread.h>


using namespace v8;
using namespace kyotocabinet;
namespace kc = kyotocabinet;


#define KC_CUR_REQ_INIT(Type, Cursor, Key, Value, Step, Writable) \
  req->type = Type;                                               \
  req->wrapcur = Cursor;                                          \
  req->result = PolyDB::Error::SUCCESS;                           \
  req->key = Key;                                                 \
  req->value = Value;                                             \
  req->step = Step;                                               \
  req->writable = Writable;                                       \
  req->visitor.Clear();                                           \
  req->cb.Clear();                                                \
  TRACE("type = %d\n", req->type);                                \


// request type
enum kc_cur_req_type {
  KC_CUR_CREATE,
  KC_CUR_JUMP,
  KC_CUR_JUMP_BACK,
  KC_CUR_STEP,
  KC_CUR_STEP_BACK,
  KC_CUR_GET,
  KC_CUR_GET_KEY,
  KC_CUR_GET_VALUE,
  KC_CUR_REMOVE,
  KC_CUR_SEIZE,
  KC_CUR_SET_VALUE,
  KC_CUR_ACCEPT,
};

// common request field
#define KC_CUR_REQ_FIELD        \
  CursorWrap *wrapcur;          \
  kc_cur_req_type type;         \
  PolyDB::Error::Code result;   \
  Persistent<Function> cb;      \

// cursor base request
typedef struct kc_cur_req_s {
  KC_CUR_REQ_FIELD;
} kc_cur_req_t;

// cursor create request
typedef struct kc_cur_create_req_s {
  KC_CUR_REQ_FIELD;
  PolyDBWrap *wrapdb;
  CursorWrap *retcur;
  Persistent<Object> self;
} kc_cur_create_req_t;

// cursor common request
typedef struct kc_cur_cmn_req_s {
  KC_CUR_REQ_FIELD;
  char *key;
  char *value;
  bool step;
  bool writable;
  Persistent<Object> visitor;
} kc_cur_cmn_req_t;


Persistent<Function> CursorWrap::ctor;


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

void CursorWrap::SendAsyncRequest(void *req) {
  assert(req != NULL);

  uv_work_t *uv_req = reinterpret_cast<uv_work_t*>(malloc(sizeof(uv_work_t)));
  assert(uv_req != NULL);
  uv_req->data = req;
  TRACE("uv_work_t = %p, type = %d\n", uv_req, ((kc_cur_req_t *)req)->type);

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, (uv_after_work_cb)OnWorkDone);
  TRACE("uv_queue_work: ret=%d\n", ret);
}


Handle<Value> CursorWrap::New(const Arguments &args) {
  HandleScope scope;
  TRACE("New\n");

  if ((args.Length() == 0) || (args.Length() == 1 && args[0]->IsFunction())) {
    ThrowException(Exception::Error(String::New("Invalid parameter")));
    return scope.Close(args.This());
  }

  Local<String> ctor_sym = String::NewSymbol("constructor");
  Local<String> name_sym = String::NewSymbol("name");
  String::Utf8Value ctorName(
    args[0]->ToObject()->Get(ctor_sym)->ToObject()->Get(name_sym)->ToString()
  );
  if (strcmp("DB", *ctorName)) {
    ThrowException(Exception::Error(String::New("Invalid parameter")));
    return scope.Close(args.This());
  }

  if (args.Length() == 1 && !args[0]->IsFunction()) {
    PolyDBWrap *dbWrap = ObjectWrap::Unwrap<PolyDBWrap>(args[0]->ToObject());
    CursorWrap *cursorWrap = new CursorWrap(dbWrap->Cursor());
    cursorWrap->Wrap(args.This());
  } else {
    kc_cur_create_req_t *req = 
      reinterpret_cast<kc_cur_create_req_t*>(malloc(sizeof(kc_cur_create_req_t)));
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

    SendAsyncRequest(req);
  }

  return scope.Close(args.This());
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
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_JUMP, wrapCur, NULL, NULL, false, false);

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

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
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
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_JUMP_BACK, wrapCur, NULL, NULL, false, false);

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

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
}

Handle<Value> CursorWrap::Step(const Arguments &args) {
  HandleScope scope;
  TRACE("Step\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && !args[0]->IsFunction()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_STEP, wrapCur, NULL, NULL, false, false);

  req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
}

Handle<Value> CursorWrap::StepBack(const Arguments &args) {
  HandleScope scope;
  TRACE("StepBack\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && !args[0]->IsFunction()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_STEP_BACK, wrapCur, NULL, NULL, false, false);

  req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
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
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_GET, wrapCur, NULL, NULL, false, false);

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

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
}

Handle<Value> CursorWrap::GetKey(const Arguments &args) {
  HandleScope scope;
  TRACE("GetKey\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsBoolean() & !args[0]->IsFunction())) ||
       (args.Length() == 2 && (!args[0]->IsBoolean() | !args[1]->IsFunction())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_GET_KEY, wrapCur, NULL, NULL, false, false);

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

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
}

Handle<Value> CursorWrap::GetValue(const Arguments &args) {
  HandleScope scope;
  TRACE("GetValue\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsBoolean() & !args[0]->IsFunction())) ||
       (args.Length() == 2 && (!args[0]->IsBoolean() | !args[1]->IsFunction())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_GET_VALUE, wrapCur, NULL, NULL, false, false);

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

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
}

Handle<Value> CursorWrap::Remove(const Arguments &args) {
  HandleScope scope;
  TRACE("Remove\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && !args[0]->IsFunction()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_REMOVE, wrapCur, NULL, NULL, false, false);

  req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
}

Handle<Value> CursorWrap::Seize(const Arguments &args) {
  HandleScope scope;
  TRACE("Seize\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && !args[0]->IsFunction()) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_SEIZE, wrapCur, NULL, NULL, false, false);

  req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
}

Handle<Value> CursorWrap::SetValue(const Arguments &args) {
  HandleScope scope;
  TRACE("SetValue\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  if ( (args.Length() == 0) || (args.Length() == 1) ||
       (args.Length() == 2 && (!args[0]->IsString() | 
                              (!args[1]->IsBoolean() & !args[1]->IsFunction()))) ||
       (args.Length() == 3 && (!args[0]->IsString() | 
                               !args[1]->IsBoolean() | !args[2]->IsFunction())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_SET_VALUE, wrapCur, NULL, NULL, false, false);

  if (args.Length() == 2) {
    if (args[0]->IsBoolean()) {
      req->step = args[0]->BooleanValue();
    } else {
      String::Utf8Value value(args[0]->ToString());
      req->value = kc::strdup(*value);
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  } else {
    String::Utf8Value value(args[0]->ToString());
    req->value = kc::strdup(*value);
    req->step = args[1]->BooleanValue();
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[2]));
  }

  SendAsyncRequest(req);

  wrapCur->Ref();

  return scope.Close(args.This());
}

Handle<Value> CursorWrap::Accept(const Arguments &args) {
  HandleScope scope;
  TRACE("Accept\n");

  CursorWrap *wrapCur = ObjectWrap::Unwrap<CursorWrap>(args.This());
  assert(wrapCur != NULL);

  Local<String> visitor_sym = String::NewSymbol("visitor");
  Local<String> writable_sym = String::NewSymbol("writable");
  Local<String> step_sym = String::NewSymbol("step");
  if ( (args.Length() == 0) ||
       (args.Length() == 1 && (!args[0]->IsObject()) | !args[0]->IsFunction()) ||
       (args.Length() == 1 && args[0]->IsObject() && (!args[0]->ToObject()->Has(visitor_sym) || !args[0]->ToObject()->Get(visitor_sym)->IsObject())) ||
       (args.Length() == 1 && args[0]->IsObject() && (args[0]->ToObject()->Has(writable_sym) & !args[0]->ToObject()->Get(writable_sym)->IsBoolean())) ||
       (args.Length() == 1 && args[0]->IsObject() && (args[0]->ToObject()->Has(step_sym) & !args[0]->ToObject()->Get(step_sym)->IsBoolean())) ||
       (args.Length() == 2 && (!args[0]->IsObject() | !args[1]->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && (!args[0]->ToObject()->Has(visitor_sym) || !args[0]->ToObject()->Get(visitor_sym)->IsFunction())) ||
       (args.Length() == 2 && args[0]->IsObject() && (args[0]->ToObject()->Has(step_sym) & !args[0]->ToObject()->Get(step_sym)->IsBoolean())) ||
       (args.Length() == 2 && args[0]->IsObject() && (args[0]->ToObject()->Has(writable_sym) & !args[0]->ToObject()->Get(writable_sym)->IsBoolean())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = 
    reinterpret_cast<kc_cur_cmn_req_t*>(malloc(sizeof(kc_cur_cmn_req_t)));
  assert(req != NULL);
  KC_CUR_REQ_INIT(KC_CUR_ACCEPT, wrapCur, NULL, NULL, false, true);

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(visitor_sym)) {
        req->visitor = Persistent<Object>::New(
          Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym))
        );
      }
      if (args[0]->ToObject()->Has(writable_sym)) {
        req->writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
      }
      if (args[0]->ToObject()->Has(step_sym)) {
        req->step = args[0]->ToObject()->Get(step_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(visitor_sym)) {
      req->visitor = Persistent<Object>::New(
        Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym))
      );
    }
    if (args[0]->ToObject()->Has(writable_sym)) {
      req->writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
    }
    if (args[0]->ToObject()->Has(step_sym)) {
      req->step = args[0]->ToObject()->Get(step_sym)->BooleanValue();
    }
    req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  SendAsyncRequest(req);

  wrapCur->Ref();
  return scope.Close(args.This());
}


void CursorWrap::OnWork(uv_work_t *work_req) {
  TRACE("argument: work_req=%p\n", work_req);

  kc_cur_req_t *req = reinterpret_cast<kc_cur_req_t*>(work_req->data);
  assert(req != NULL);

  // do operation
  switch (req->type) {
    case KC_CUR_CREATE:
      {
        kc_cur_create_req_t *cur_req = reinterpret_cast<kc_cur_create_req_t*>(work_req->data);
        cur_req->retcur = new CursorWrap(cur_req->wrapdb->Cursor());
        break;
      }
    case KC_CUR_JUMP:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
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
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
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
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (!wrapCur->cursor_->step()) {
          cur_req->result = wrapCur->GetErrorCode();
        }
        break;
      }
    case KC_CUR_STEP_BACK:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (!wrapCur->cursor_->step_back()) {
          cur_req->result = wrapCur->GetErrorCode();
        }
        break;
      }
    case KC_CUR_GET:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
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
    case KC_CUR_GET_KEY:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        TRACE("cursor get_key: step = %d\n", cur_req->step);
        size_t key_size;
        char *key = wrapCur->cursor_->get_key(&key_size, cur_req->step);
        TRACE("cursor->get_key: key = %s(%p)\n", key, key);
        if (key == NULL) {
          cur_req->result = wrapCur->GetErrorCode();
        } else {
          cur_req->key = key;
        }
        break;
      }
    case KC_CUR_GET_VALUE:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        TRACE("cursor get_value: step = %d\n", cur_req->step);
        size_t value_size;
        char *value = wrapCur->cursor_->get_value(&value_size, cur_req->step);
        TRACE("cursor->get_value: value = %s(%p)\n", value, value);
        if (value == NULL) {
          cur_req->result = wrapCur->GetErrorCode();
        } else {
          cur_req->value = value;
        }
        break;
      }
    case KC_CUR_REMOVE:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (!wrapCur->cursor_->remove()) {
          cur_req->result = wrapCur->GetErrorCode();
        }
        break;
      }
    case KC_CUR_SEIZE:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        const char *value = NULL;
        size_t key_size, value_size;
        char *key = wrapCur->cursor_->seize(&key_size, &value, &value_size);
        TRACE("cursor->seize: key = %s(%p), value = %s(%p)\n", key, key, value, value);
        if (key == NULL || value == NULL) {
          cur_req->result = wrapCur->GetErrorCode();
        } else {
          cur_req->key = key;
          cur_req->value = kc::strdup((char *)value);
        }
        break;
      }
    case KC_CUR_SET_VALUE:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (!wrapCur->cursor_->set_value(cur_req->value, strlen(cur_req->value), cur_req->step)) {
          cur_req->result = wrapCur->GetErrorCode();
        }
        break;
      }
    case KC_CUR_ACCEPT:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        AsyncVisitor visitor(cur_req->visitor, cur_req->writable);
        TRACE("cursor->accept: writable = %d, step = %d\n", cur_req->writable, cur_req->step);
        if (!wrapCur->cursor_->accept(&visitor, cur_req->writable, cur_req->step)) {
          TRACE("cursor->accept failed\n");
          cur_req->result = wrapCur->GetErrorCode();
        }
        TRACE("cursor->accept done\n");
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

  kc_cur_req_t *req = reinterpret_cast<kc_cur_req_t*>(work_req->data);
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
      DEFINE_JS_CONSTANT(obj, "code", req->result);
      argv[argc] = err;
    }
    argc++;
  }

  // other data to callback argument.
  switch (req->type) {
    case KC_CUR_CREATE:
      {
        kc_cur_create_req_t *cur_req = reinterpret_cast<kc_cur_create_req_t*>(work_req->data);
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
    case KC_CUR_SEIZE:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
          argv[argc++] = String::New(cur_req->key, strlen(cur_req->key));
          argv[argc++] = String::New(cur_req->value, strlen(cur_req->value));
        }
        break;
      }
    case KC_CUR_GET_KEY:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
          argv[argc++] = String::New(cur_req->key, strlen(cur_req->key));
        }
        break;
      }
    case KC_CUR_GET_VALUE:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
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
        kc_cur_create_req_t *cur_req = reinterpret_cast<kc_cur_create_req_t*>(work_req->data);
        cur_req->wrapdb->Unref();
        cur_req->wrapdb = NULL;
        cur_req->self.Dispose();
        cur_req->retcur = NULL;
        free(cur_req);
        break;
      }
    case KC_CUR_ACCEPT:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        cur_req->visitor.Dispose();
        free(cur_req);
        break;
      }
    case KC_CUR_JUMP:
    case KC_CUR_JUMP_BACK:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(cur_req, key);
        free(cur_req);
        break;
      }
    case KC_CUR_STEP:
    case KC_CUR_STEP_BACK:
    case KC_CUR_REMOVE:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        free(cur_req);
        break;
      }
    case KC_CUR_GET:
    case KC_CUR_SEIZE:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(cur_req, key);
        SAFE_REQ_ATTR_FREE(cur_req, value);
        free(cur_req);
        break;
      }
    case KC_CUR_GET_KEY:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(cur_req, key);
        free(cur_req);
        break;
      }
    case KC_CUR_GET_VALUE:
    case KC_CUR_SET_VALUE:
      {
        kc_cur_cmn_req_t *cur_req = reinterpret_cast<kc_cur_cmn_req_t*>(work_req->data);
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
  DEFINE_JS_METHOD(prottpl, "jump", Jump);
  DEFINE_JS_METHOD(prottpl, "jump_back", JumpBack);
  DEFINE_JS_METHOD(prottpl, "step", Step);
  DEFINE_JS_METHOD(prottpl, "step_back", StepBack);
  DEFINE_JS_METHOD(prottpl, "get", Get);
  DEFINE_JS_METHOD(prottpl, "get_key", GetKey);
  DEFINE_JS_METHOD(prottpl, "get_value", GetValue);
  DEFINE_JS_METHOD(prottpl, "remove", Remove);
  DEFINE_JS_METHOD(prottpl, "seize", Seize);
  DEFINE_JS_METHOD(prottpl, "set_value", SetValue);
  DEFINE_JS_METHOD(prottpl, "accept", Accept);

  ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("Cursor"), ctor);

  // define function(s)
  DEFINE_JS_METHOD(ctor, "create", Create);
}

