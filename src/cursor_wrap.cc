/*
 * cursor wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include "cursor_wrap.h" 
#include "polydb_wrap.h"
#include "debug.h"
#include <assert.h>

using namespace v8;
using namespace kyotocabinet;
namespace kc = kyotocabinet;


// request type
enum kc_cur_req_type {
  KC_CUR_CREATE,
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
  Persistent<Object> wrapdb;
  CursorWrap *retcur;
  Persistent<Object> self;
} kc_cur_create_req_t;


CursorWrap::CursorWrap(PolyDB::Cursor *cursor) : cursor_(cursor) {
  TRACE("ctor: cursor_ = %p\n", cursor_);
  assert(cursor_ != NULL);
}

CursorWrap::~CursorWrap() {
  TRACE("destor: cursor_ = %p\n", cursor_);
  if (cursor_) {
    delete cursor_;
    cursor_ = NULL;
  }
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
    req->wrapdb.Clear();
    req->retcur = NULL;
    req->self.Clear();
    req->cb.Clear();

    req->wrapdb = Persistent<Object>::New(Handle<Object>::Cast(args[0]));
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

Handle<Value> CursorWrap::Get(const Arguments &args) {
  HandleScope scope;
  TRACE("Get\n");

  CursorWrap *curWrap = ObjectWrap::Unwrap<CursorWrap>(args.This());

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
        PolyDBWrap *dbWrap = ObjectWrap::Unwrap<PolyDBWrap>(cur_req->wrapdb);
        cur_req->retcur = new CursorWrap(dbWrap->Cursor());
        break;
      }
    default:
      assert(0);
      break;
  }
}

Local<Value> CursorWrap::MakeErrorObject(PolyDB::Error::Code result) {
  const char *name = PolyDB::Error::codename(result);
  Local<String> message = String::NewSymbol(name);
  Local<Value> err = Exception::Error(message);
  Local<Object> obj = err->ToObject();
  obj->Set(String::NewSymbol("code"), Integer::New(result), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  return err;
}

void CursorWrap::OnWorkDone(uv_work_t *work_req) {
  HandleScope scope;
  TRACE("argument: work_req=%p\n", work_req);

  kc_cur_req_type_t *req = static_cast<kc_cur_req_type_t*>(work_req->data);
  assert(req != NULL);

  // init callback arguments.
  int argc = 0;
  Local<Value> argv[2] = { 
    Local<Value>::New(Null()),
    Local<Value>::New(Null()),
  };

  // set error to callback arguments.
  /*
  if (req->result != PolyDB::Error::SUCCESS) {
    argv[argc] = MakeErrorObject(req->result);
  }
  argc++;
  */

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
          argv[argc++] = cur_req->self->ToObject();
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
        cur_req->self.Dispose();
        cur_req->wrapdb.Dispose();
        cur_req->retcur = NULL;
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
  prottpl->Set(String::NewSymbol("get"), FunctionTemplate::New(Get)->GetFunction());

  ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("Cursor"), ctor);

  // define function(s)
  ctor->Set(String::NewSymbol("create"), FunctionTemplate::New(Create)->GetFunction());
}

