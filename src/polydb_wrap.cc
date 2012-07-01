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

// request type
enum kc_req_type {
  KC_OPEN,
  KC_CLOSE,
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
  String::Utf8Value str(args[0]->ToObject()->Get(String::NewSymbol("path"))->ToString());
  open_req->path = *str;
  open_req->mode = args[0]->ToObject()->Get(String::NewSymbol("mode"))->Uint32Value();
  open_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));

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
  close_req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
  
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = close_req;

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

  switch (req->type) {
    case KC_OPEN:
      {
        kc_open_req_t *open_req = static_cast<kc_open_req_t *>(work_req->data);
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
    default:
      assert(0);
  }
}

void PolyDBWrap::OnWorkDone(uv_work_t *work_req) {
  TRACE("argument: work_req=%p\n", work_req);
  HandleScope scope;

  kc_req_t *req = static_cast<kc_req_t *>(work_req->data);
  PolyDBWrap *wrapdb = req->wrapdb;
  assert(wrapdb != NULL);

  Local<Value> argv[1];
  if (req->result != PolyDB::Error::SUCCESS) {
    const char *name = PolyDB::Error::codename(req->result);
    Local<String> message = String::NewSymbol(name);
    Local<Value> err = Exception::Error(message);
    Local<Object> obj = err->ToObject();
    obj->Set(String::NewSymbol("code"), Integer::New(req->result), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
    argv[0] = err;
  } else {
    argv[0] = Local<Value>::New(Null());
  }

  TryCatch try_catch;
  //req->cb->Call(Context::GetCurrent()->Global(), 1, argv);
  MakeCallback(wrapdb->handle_, req->cb, 1, argv);
  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }

  wrapdb->Unref();
  req->cb.Dispose();

  switch (req->type) {
    case KC_OPEN:
      {
        kc_open_req_t *open_req = static_cast<kc_open_req_t *>(work_req->data);
        free(open_req);
        break;
      }
    case KC_CLOSE:
      free(req);
      break;
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

