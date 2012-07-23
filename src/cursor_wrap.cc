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
  Persistent<Object> visitor;
} kc_cur_cmn_req_t;


static pthread_mutex_t cursor_access = PTHREAD_MUTEX_INITIALIZER;


class AsyncCursorVisitor : public PolyDB::Visitor {
public:
  pthread_cond_t cond_;
  Persistent<Object> visitor_;
  bool writable_;
  char *key_;
  char *value_;
  size_t key_size_;
  size_t value_size_;
  char *rv_;
  size_t sp_;

  explicit AsyncCursorVisitor(uv_loop_t *loop) : loop_(loop), 
                                                 writable_(false), rv_(NULL), sp_(-1) {
    TRACE("ctor: %p\n", this);
    visitor_.Clear();
    pthread_cond_init(&cond_, NULL);
    TRACE("arguments: async_ = %p\n", &async_);
  }
  ~AsyncCursorVisitor() {
    TRACE("destor\n");
    pthread_cond_destroy(&cond_);
    loop_ = NULL;
  }
private:
  uv_loop_t *loop_;
  uv_async_t async_;

  const char* visit_full(const char *kbuf, size_t ksiz,
                         const char *vbuf, size_t vsiz, size_t *sp) {
    //HandleScope scope;
    TRACE("arguments: kbuf = %s, ksiz = %d, vbuf = %s, vsiz = %d, sp = %d(%p)\n", kbuf, ksiz, vbuf, vsiz, *sp, sp);
    rv_ = (char *)NOP;
    sp_ = *sp;
    key_ = (char *)kbuf;
    key_size_ = ksiz;
    value_ = (char *)vbuf;
    value_size_ = vsiz;

    uv_async_init(loop_, &async_, AsyncCallback);
    async_.data = this;
    uv_ref((uv_handle_t *)&async_);

    TRACE("before: rv_ = %s(%p), sp_ = %d\n", rv_, rv_, sp_);
    uv_async_send(&async_);

    TRACE("wating ... \n");
    pthread_cond_wait(&cond_, &cursor_access);
    TRACE("... callback done\n");

    TRACE("after: rv_ = %p, sp_ = %d\n", rv_, sp_);
    const char *rv = (const char *)rv_;
    if ((const char *)rv_ != NOP && (const char *)rv_ != REMOVE) {
      *sp = sp_;
      TRACE("sp = %d(%p)\n", *sp, sp);
    }
    /*
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
    */
    
    return rv;
  }

  static void AsyncCloseCallback(uv_handle_t *handle) {
    TRACE("handle = %p\n", handle);
  }

  static void AsyncCallback(uv_async_t *async, int status) {
    HandleScope scope;
    TRACE("async = %p, status = %d\n", async, status);

    AsyncCursorVisitor *visitor = static_cast<AsyncCursorVisitor*>(async->data);
    assert(visitor != NULL);
    TRACE("visitor(%p): key_ = %s, key_size_ = %d, value_ = %s, value_size_ = %d\n", 
        visitor, visitor->key_, visitor->key_size_, visitor->value_, visitor->value_size_);

    Local<Value> ret;
    Local<Function> cb;
    if (!visitor->visitor_->IsFunction()) {
      visitor->rv_ = (char *)NOP;
    } else {
      cb = Local<Function>::New(Handle<Function>::Cast(visitor->visitor_));
    }

    if (!cb.IsEmpty()) {
      Local<Value> argv[2] = { 
        String::New(visitor->key_, visitor->key_size_),
        String::New(visitor->value_, visitor->value_size_),
      };
      TryCatch try_catch;
      ret = cb->Call(Context::GetCurrent()->Global(), 2, argv);
      if (try_catch.HasCaught()) {
        FatalException(try_catch);
      }

      if (visitor->writable_) {
        if (!ret.IsEmpty()) {
          if (ret->IsNumber()) {
            int64_t num_ret = ret->IntegerValue();
            if (num_ret == 1) {
              const char *rv = REMOVE;
              visitor->rv_ = (char *)rv;
            }
          } else if (ret->IsString()) {
            String::Utf8Value str_ret(ret->ToString());
            if (*str_ret) {
              visitor->rv_ = kc::strdup(*str_ret);
              visitor->sp_ = strlen(visitor->rv_);
            }
          }
        }
      } else {
        visitor->rv_ = NULL;
      }
    }

    uv_unref((uv_handle_t *)async);
    uv_close((uv_handle_t *)async, AsyncCloseCallback);
    async->data = NULL;

    TRACE("signal notify ... : rv_ = %p, sp_ = %d\n", visitor->rv_, visitor->sp_);
    pthread_cond_signal(&visitor->cond_);
    TRACE("... notify done\n");
  }
};


CursorWrap::CursorWrap(PolyDB::Cursor *cursor) : cursor_(cursor) {
  TRACE("ctor: cursor_ = %p\n", cursor_);
  assert(cursor_ != NULL);
  wrapdb_ = NULL;
  visitor_ = new AsyncCursorVisitor(uv_default_loop());
}

CursorWrap::~CursorWrap() {
  TRACE("destor: wrapdb_ = %p, cursor_ = %p, visitor_ = %p\n", wrapdb_, cursor_, visitor_);
  if (visitor_) {
    delete visitor_;
    visitor_ = NULL;
  }
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


void CursorWrap::SendAsyncRequest(void *req) {
  uv_work_t *uv_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  uv_req->data = req;

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
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
  String::Utf8Value ctorName(args[0]->ToObject()->Get(ctor_sym)->ToObject()->Get(name_sym)->ToString());
  if (strcmp("DB", *ctorName)) {
    ThrowException(Exception::Error(String::New("Invalid parameter")));
    return scope.Close(args.This());
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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
       (args.Length() == 2 && (!args[0]->IsString() | (!args[1]->IsBoolean() & !args[1]->IsFunction()))) ||
       (args.Length() == 3 && (!args[0]->IsString() | !args[1]->IsBoolean() | !args[2]->IsFunction())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
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

  kc_cur_cmn_req_t *req = (kc_cur_cmn_req_t *)malloc(sizeof(kc_cur_cmn_req_t));
  KC_CUR_REQ_INIT(KC_CUR_ACCEPT, wrapCur, NULL, NULL, false, true);

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
      if (args[0]->ToObject()->Has(step_sym)) {
        req->step = args[0]->ToObject()->Get(step_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(visitor_sym)) {
      req->visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
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
  /*
  PolyDB::Error::Code result = PolyDB::Error::SUCCESS;
  Persistent<Function> cb;
  bool writable = true;
  bool step = false;
  Persistent<Object> visitor;
  cb.Clear();
  visitor.Clear();

  if (args.Length() == 1) {
    if (args[0]->IsFunction()) {
      cb = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
    } else if (args[0]->IsObject()) {
      if (args[0]->ToObject()->Has(visitor_sym)) {
        visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
      }
      if (args[0]->ToObject()->Has(writable_sym)) {
        writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
      }
      if (args[0]->ToObject()->Has(step_sym)) {
        step = args[0]->ToObject()->Get(step_sym)->BooleanValue();
      }
    }
  } else {
    if (args[0]->ToObject()->Has(visitor_sym)) {
      visitor = Persistent<Object>::New(Handle<Object>::Cast(args[0]->ToObject()->Get(visitor_sym)));
    }
    if (args[0]->ToObject()->Has(writable_sym)) {
      writable = args[0]->ToObject()->Get(writable_sym)->BooleanValue();
    }
    if (args[0]->ToObject()->Has(step_sym)) {
      step = args[0]->ToObject()->Get(step_sym)->BooleanValue();
    }
    cb = Persistent<Function>::New(Handle<Function>::Cast(args[1]));
  }

  // TODO' should be non-blocking implements ...
  // execute
  if (visitor.IsEmpty()) {
    result = PolyDB::Error::INVALID;
  } else {
    wrapCur->visitor_->writable_ = writable;
    wrapCur->visitor_->visitor_.Clear();
    wrapCur->visitor_->visitor_ = visitor;
    if (!wrapCur->cursor_->accept(wrapCur->visitor_, writable, step)) {
      TRACE("cursor->accept failed\n");
      result = wrapCur->GetErrorCode();
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
    MakeCallback(wrapCur->handle_, cb, 1, argv);
    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }
  } 
  */

  return scope.Close(args.This());
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
    case KC_CUR_GET_KEY:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
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
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
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
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (!wrapCur->cursor_->remove()) {
          cur_req->result = wrapCur->GetErrorCode();
        }
        break;
      }
    case KC_CUR_SEIZE:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
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
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        if (!wrapCur->cursor_->set_value(cur_req->value, strlen(cur_req->value), cur_req->step)) {
          cur_req->result = wrapCur->GetErrorCode();
        }
        break;
      }
    case KC_CUR_ACCEPT:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        CursorWrap *wrapCur = cur_req->wrapcur;
        wrapCur->visitor_->writable_ = cur_req->writable;
        wrapCur->visitor_->visitor_.Clear();
        wrapCur->visitor_->visitor_ = cur_req->visitor;
        TRACE("cursor->accept: writable = %d, step = %d\n", cur_req->writable, cur_req->step);
        if (!wrapCur->cursor_->accept(wrapCur->visitor_, cur_req->writable, cur_req->step)) {
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
    case KC_CUR_SEIZE:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
          argv[argc++] = String::New(cur_req->key, strlen(cur_req->key));
          argv[argc++] = String::New(cur_req->value, strlen(cur_req->value));
        }
        break;
      }
    case KC_CUR_GET_KEY:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
          argv[argc++] = String::New(cur_req->key, strlen(cur_req->key));
        }
        break;
      }
    case KC_CUR_GET_VALUE:
      {
        if (req->result == PolyDB::Error::SUCCESS) {
          kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
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
    case KC_CUR_ACCEPT:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        cur_req->visitor.Dispose();
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
    case KC_CUR_REMOVE:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        free(cur_req);
        break;
      }
    case KC_CUR_GET:
    case KC_CUR_SEIZE:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(cur_req, key);
        SAFE_REQ_ATTR_FREE(cur_req, value);
        free(cur_req);
        break;
      }
    case KC_CUR_GET_KEY:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
        SAFE_REQ_ATTR_FREE(cur_req, key);
        free(cur_req);
        break;
      }
    case KC_CUR_GET_VALUE:
    case KC_CUR_SET_VALUE:
      {
        kc_cur_cmn_req_t *cur_req = static_cast<kc_cur_cmn_req_t*>(work_req->data);
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
  prottpl->Set(String::NewSymbol("get_key"), FunctionTemplate::New(GetKey)->GetFunction());
  prottpl->Set(String::NewSymbol("get_value"), FunctionTemplate::New(GetValue)->GetFunction());
  prottpl->Set(String::NewSymbol("remove"), FunctionTemplate::New(Remove)->GetFunction());
  prottpl->Set(String::NewSymbol("seize"), FunctionTemplate::New(Seize)->GetFunction());
  prottpl->Set(String::NewSymbol("set_value"), FunctionTemplate::New(SetValue)->GetFunction());
  prottpl->Set(String::NewSymbol("accept"), FunctionTemplate::New(Accept)->GetFunction());

  ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("Cursor"), ctor);

  // define function(s)
  ctor->Set(String::NewSymbol("create"), FunctionTemplate::New(Create)->GetFunction());
}

