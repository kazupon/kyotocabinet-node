/*
 * mapreduce wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include "mapreduce_wrap.h" 
#include "polydb_wrap.h"
#include "debug.h"
#include "utils.h"
#include <assert.h>
#include <kcthread.h>

using namespace v8;
using namespace kyotocabinet;
namespace kc = kyotocabinet;


static pthread_mutex_t async_mapreduce_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t async_mapreduce_cond = PTHREAD_COND_INITIALIZER;
static uv_async_t async_mapreduce_req_notifier;
static struct timespec ts;


// request type
enum kc_mapreduce_req_kind_t {
  KC_MAPREDUCE_EXECUTE,
};

// async request type
enum kc_async_mapreduce_kind_t {
  KC_ASYNC_MAPREDUCE_MAP,
  KC_ASYNC_MAPREDUCE_REDUCE,
  KC_ASYNC_MAPREDUCE_LOG,
  KC_ASYNC_MAPREDUCE_PRE,
  KC_ASYNC_MAPREDUCE_MID,
  KC_ASYNC_MAPREDUCE_POST,
};

// common request fields
#define KC_MAPREDUCE_REQ_FIELD  \
  MapReduceWrap *wrapmapreduce; \
  kc_mapreduce_req_kind_t type; \
  PolyDB::Error::Code result;   \
  Persistent<Function> cb;      \

// common async request fields
#define KC_ASYNC_MAPREDUCE_REQ_FIELD  \
  Persistent<Function> cb;            \
  AtomicInt64 done;                   \
  kc_async_mapreduce_kind_t type;     \

// mapreduce request
typedef struct kc_mapreduce_req_s {
  KC_MAPREDUCE_REQ_FIELD;
  PolyDBWrap *wrapdb;
  char *tmppath;
  uint32_t opts;
  Persistent<Function> emit;
  Persistent<Function> iter;
} kc_mapreduce_req_t;

// mapreduce async request
typedef struct kc_async_mapreduce_req_s {
  KC_ASYNC_MAPREDUCE_REQ_FIELD;
  char *field1;
  size_t field1_size;
  char *field2;
  size_t field2_size;
  bool ret;
  MapReduceWrap *wrapmapreduce;
} kc_async_mapreduce_req_t;


static void send_request_and_wait_result(
  kc_async_mapreduce_req_t *req, uv_async_t *notifier, pthread_mutex_t *mtx, 
  pthread_cond_t *cond, struct timespec *ts) {
  notifier->data = req;
  int32_t ret = uv_async_send(notifier);
  TRACE("uv_async_send: ret = %d\n", ret);

  TRACE("callback wating ... \n");
  do {
    int32_t code = pthread_cond_timedwait(cond, mtx, ts);
    assert(code == 0 || code == ETIMEDOUT || code == EINTR);
  } while (req->done.get() == 0);
  TRACE("... callback done\n");
}


MapReduceWrap::MapReduceWrap(
    Persistent<Function> &map,
    Persistent<Function> &reduce,
    Persistent<Function> &log,
    Persistent<Function> &pre,
    Persistent<Function> &mid,
    Persistent<Function> &post,
    int32_t dbnum,
    int64_t clim,
    int64_t cbnum
  )
  : map_(map), reduce_(reduce), log_(log), pre_(pre), mid_(mid), post_(post),
    dbnum_(dbnum), clim_(clim), cbnum_(cbnum) {
  TRACE("arguments: dbnum_ = %d, clim_ = %lld, cbnum_ = %lld\n", dbnum_, clim_, cbnum_);
  TRACE(
    "arguments empty state: map_ = %d, reduce_ = %d, log_ = %d, "
    "pre_ = %d, mid_ = %d, post_ = %d\n", 
    map_.IsEmpty(), reduce_.IsEmpty(), log_.IsEmpty(), 
    pre_.IsEmpty(), mid_.IsEmpty(), post_.IsEmpty()
  );

  emit_.Clear();
  iter_.Clear();
  val_iter_ = NULL;

  TRACE("this: %p\n", this);
}

MapReduceWrap::~MapReduceWrap() {
  TRACE(
    "arguments empty state: map_ = %d, reduce_ = %d, log_ = %d, "
    "pre_ = %d, mid_ = %d, post_ = %d\n", 
    map_.IsEmpty(), reduce_.IsEmpty(), log_.IsEmpty(), 
    pre_.IsEmpty(), mid_.IsEmpty(), post_.IsEmpty()
  );
  TRACE("this: %p\n", this);

  map_.Dispose();
  reduce_.Dispose();
  log_.Dispose();
  pre_.Dispose();
  mid_.Dispose();
  post_.Dispose();
}

bool MapReduceWrap::map(const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz) {
  TRACE("arguments: kbuf = %s, ksiz = %ld, vbuf = %s, vsiz = %ld\n", kbuf, ksiz, vbuf, vsiz);

  TRACE("lock ...\n");
  pthread_mutex_lock(&async_mapreduce_mtx);

  kc_async_mapreduce_req_t *req = 
    reinterpret_cast<kc_async_mapreduce_req_t*>(malloc(sizeof(kc_async_mapreduce_req_t)));
  assert(req != NULL);
  req->cb = map_;
  req->done.set(0);
  req->type = KC_ASYNC_MAPREDUCE_MAP;
  req->field1 = const_cast<char*>(kbuf);
  req->field1_size = ksiz;
  req->field2 = const_cast<char*>(vbuf);
  req->field2_size = vsiz;
  req->ret = false;
  req->wrapmapreduce = this;

  send_request_and_wait_result(
    req, &async_mapreduce_req_notifier, 
    &async_mapreduce_mtx, &async_mapreduce_cond, &ts
  );

  bool rv = req->ret;
  free(req);

  pthread_mutex_unlock(&async_mapreduce_mtx);
  TRACE("... unlock\n");

  return rv;
}

bool MapReduceWrap::reduce(const char *kbuf, size_t ksiz, MapReduce::ValueIterator *iter) {
  TRACE("arguments: kbuf = %s, ksiz = %ld, iter = %p\n", kbuf, ksiz, iter);

  TRACE("lock ...\n");
  pthread_mutex_lock(&async_mapreduce_mtx);

  kc_async_mapreduce_req_t *req = 
    reinterpret_cast<kc_async_mapreduce_req_t*>(malloc(sizeof(kc_async_mapreduce_req_t)));
  assert(req != NULL);
  req->cb = reduce_;
  req->done.set(0);
  req->type = KC_ASYNC_MAPREDUCE_REDUCE;
  req->field1 = const_cast<char*>(kbuf);
  req->field1_size = ksiz;
  req->field2 = NULL;
  req->field2_size = 0;
  req->ret = false;
  val_iter_ = iter;
  req->wrapmapreduce = this;

  send_request_and_wait_result(
    req, &async_mapreduce_req_notifier, 
    &async_mapreduce_mtx, &async_mapreduce_cond, &ts
  );

  val_iter_ = NULL;
  bool rv = req->ret;
  free(req);

  pthread_mutex_unlock(&async_mapreduce_mtx);
  TRACE("... unlock\n");

  return rv;
}

bool MapReduceWrap::log(const char *name, const char *message) {
  TRACE("arguments: name = %s, message = %s\n", name, message);

  pthread_mutex_lock(&async_mapreduce_mtx);

  if (log_.IsEmpty()) {
    pthread_mutex_unlock(&async_mapreduce_mtx);
    return true;
  }

  kc_async_mapreduce_req_t *req = 
    reinterpret_cast<kc_async_mapreduce_req_t*>(malloc(sizeof(kc_async_mapreduce_req_t)));
  assert(req != NULL);
  req->cb = log_;
  req->done.set(0);
  req->type = KC_ASYNC_MAPREDUCE_LOG;
  req->field1 = const_cast<char*>(name);
  req->field1_size = strlen(name);
  req->field2 = const_cast<char*>(message);
  req->field2_size = strlen(message);
  req->ret = false;
  val_iter_ = NULL;
  req->wrapmapreduce = this;

  send_request_and_wait_result(
    req, &async_mapreduce_req_notifier, 
    &async_mapreduce_mtx, &async_mapreduce_cond, &ts
  );

  bool rv = req->ret;
  free(req);

  pthread_mutex_unlock(&async_mapreduce_mtx);

  return rv;
}

bool MapReduceWrap::preprocess() {
  pthread_mutex_lock(&async_mapreduce_mtx);

  if (pre_.IsEmpty()) {
    pthread_mutex_unlock(&async_mapreduce_mtx);
    return true;
  }

  kc_async_mapreduce_req_t *req = 
    reinterpret_cast<kc_async_mapreduce_req_t*>(malloc(sizeof(kc_async_mapreduce_req_t)));
  assert(req != NULL);
  req->cb = pre_;
  req->done.set(0);
  req->type = KC_ASYNC_MAPREDUCE_PRE;
  req->field1 = NULL;
  req->field1_size = -1;
  req->field2 = NULL;
  req->field2_size = -1;
  req->ret = false;
  val_iter_ = NULL;
  req->wrapmapreduce = this;

  send_request_and_wait_result(
    req, &async_mapreduce_req_notifier, 
    &async_mapreduce_mtx, &async_mapreduce_cond, &ts
  );

  bool rv = req->ret;
  free(req);

  pthread_mutex_unlock(&async_mapreduce_mtx);
  return rv;
}

bool MapReduceWrap::midprocess() {
  pthread_mutex_lock(&async_mapreduce_mtx);

  if (mid_.IsEmpty()) {
    pthread_mutex_unlock(&async_mapreduce_mtx);
    return true;
  }

  kc_async_mapreduce_req_t *req = 
    reinterpret_cast<kc_async_mapreduce_req_t*>(malloc(sizeof(kc_async_mapreduce_req_t)));
  assert(req != NULL);
  req->cb = mid_;
  req->done.set(0);
  req->type = KC_ASYNC_MAPREDUCE_MID;
  req->field1 = NULL;
  req->field1_size = -1;
  req->field2 = NULL;
  req->field2_size = -1;
  req->ret = false;
  val_iter_ = NULL;
  req->wrapmapreduce = this;

  send_request_and_wait_result(
    req, &async_mapreduce_req_notifier, 
    &async_mapreduce_mtx, &async_mapreduce_cond, &ts
  );

  bool rv = req->ret;
  free(req);

  pthread_mutex_unlock(&async_mapreduce_mtx);
  return rv;
}

bool MapReduceWrap::postprocess() {
  pthread_mutex_lock(&async_mapreduce_mtx);

  if (post_.IsEmpty()) {
    pthread_mutex_unlock(&async_mapreduce_mtx);
    return true;
  }

  kc_async_mapreduce_req_t *req = 
    reinterpret_cast<kc_async_mapreduce_req_t*>(malloc(sizeof(kc_async_mapreduce_req_t)));
  assert(req != NULL);
  req->cb = post_;
  req->done.set(0);
  req->type = KC_ASYNC_MAPREDUCE_POST;
  req->field1 = NULL;
  req->field1_size = -1;
  req->field2 = NULL;
  req->field2_size = -1;
  req->ret = false;
  val_iter_ = NULL;
  req->wrapmapreduce = this;

  send_request_and_wait_result(
    req, &async_mapreduce_req_notifier, 
    &async_mapreduce_mtx, &async_mapreduce_cond, &ts
  );

  bool rv = req->ret;
  free(req);

  pthread_mutex_unlock(&async_mapreduce_mtx);
  return rv;
}

bool MapReduceWrap::emit_public(const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz) {
  TRACE("arguments: kbuf = %s, ksiz = %ld, vbuf = %s, vsiz = %ld\n", kbuf, ksiz, vbuf, vsiz);
  return emit(kbuf, ksiz, vbuf, vsiz);
}


Handle<Value> MapReduceWrap::New(const Arguments &args) {
  HandleScope scope;
  TRACE("New\n");
  
  Persistent<Function> map = Persistent<Function>::New(Handle<Function>::Cast(args[0]));
  Persistent<Function> reduce = Persistent<Function>::New(Handle<Function>::Cast(args[1]));

  Persistent<Function> log;
  log.Clear();
  if (args[2]->IsFunction()) {
    log = Persistent<Function>::New(Handle<Function>::Cast(args[2]));
  }
  Persistent<Function> pre;
  pre.Clear();
  if (args[3]->IsFunction()) {
    pre = Persistent<Function>::New(Handle<Function>::Cast(args[3]));
  }
  Persistent<Function> mid;
  mid.Clear();
  if (args[4]->IsFunction()) {
    mid = Persistent<Function>::New(Handle<Function>::Cast(args[4]));
  }
  Persistent<Function> post;
  post.Clear();
  if (args[5]->IsFunction()) {
    post = Persistent<Function>::New(Handle<Function>::Cast(args[5]));
  }

  int32_t dbnum = -1;
  int64_t clim = -1;
  int64_t cbnum = -1;

  MapReduceWrap *wrapMapReduce = new MapReduceWrap(
    map, reduce, log, pre, mid, post, dbnum, clim, cbnum
  );
  wrapMapReduce->Wrap(args.This());

  return scope.Close(args.This());
}

Handle<Value> MapReduceWrap::Emit(const Arguments &args) {
  HandleScope scope;
  TRACE("Emit\n");

  MapReduceWrap *wrapMapReduce = ObjectWrap::Unwrap<MapReduceWrap>(args.This());
  assert(wrapMapReduce != NULL);

  if (args.Length() < 2) {
    return scope.Close(Boolean::New(false));
  }

  String::Utf8Value key(args[0]->ToString());
  String::Utf8Value value(args[1]->ToString());
  TRACE("key = %s, value = %s\n", *key, *value);

  return scope.Close(Boolean::New(
    wrapMapReduce->emit_public(*key, strlen(*key), *value, strlen(*value))
  ));
}

Handle<Value> MapReduceWrap::Iterate(const Arguments &args) {
  HandleScope scope;
  TRACE("Iterate\n");

  MapReduceWrap *wrapMapReduce = ObjectWrap::Unwrap<MapReduceWrap>(args.This());
  assert(wrapMapReduce != NULL);
  if (!wrapMapReduce->val_iter_) {
    return scope.Close(Undefined());
  }

  size_t value_size;
  const char *value = wrapMapReduce->val_iter_->next(&value_size);
  return scope.Close(
    (value ? String::New(value, value_size) : Undefined())
  );
}

Handle<Value> MapReduceWrap::Execute(const Arguments &args) {
  HandleScope scope;
  TRACE("Execute\n");

  MapReduceWrap *wrapMapReduce = ObjectWrap::Unwrap<MapReduceWrap>(args.This());
  assert(wrapMapReduce != NULL);

  if ( (args.Length() == 0) ||
       (args.Length() == 1 && !args[0]->IsObject()) ||
       (args.Length() == 2 && (!args[0]->IsObject() | !args[1]->IsFunction())) ||
       (args.Length() == 3 && (!args[0]->IsObject() | !args[1]->IsString() | 
                               !args[2]->IsFunction())) ||
       (args.Length() == 4 && (!args[0]->IsObject() | !args[1]->IsString() | 
                               !args[2]->IsNumber() | !args[3]->IsFunction())) ) {
    ThrowException(Exception::TypeError(String::New("Bad argument")));
    return scope.Close(args.This());
  }

  Local<String> ctor_sym = String::NewSymbol("constructor");
  Local<String> name_sym = String::NewSymbol("name");
  String::Utf8Value ctorName(
    args[0]->ToObject()->Get(ctor_sym)->ToObject()->Get(name_sym)->ToString()
  );
  if (strcmp("DB", *ctorName)) {
    ThrowException(Exception::TypeError(String::New("Invalid parameter")));
    return scope.Close(args.This());
  }
  assert(args.Length() > 1);

  Local<String> emit_sym = String::NewSymbol("emit");
  Local<String> iter_sym = String::NewSymbol("iter");
  kc_mapreduce_req_t *req = 
    reinterpret_cast<kc_mapreduce_req_t*>(malloc(sizeof(kc_mapreduce_req_t)));
  assert(req != NULL);
  req->wrapmapreduce = wrapMapReduce;
  req->type = KC_MAPREDUCE_EXECUTE;
  req->result = PolyDB::Error::SUCCESS;
  req->cb.Clear();
  req->wrapdb = ObjectWrap::Unwrap<PolyDBWrap>(args[0]->ToObject());
  req->wrapdb->Ref();
  req->tmppath = NULL;
  req->opts = 0;
  req->wrapmapreduce->emit_ = Persistent<Function>::New(
    Handle<Function>::Cast(args.This()->ToObject()->Get(emit_sym))
  );
  req->wrapmapreduce->iter_ = Persistent<Function>::New(
    Handle<Function>::Cast(args.This()->ToObject()->Get(iter_sym))
  );

  int32_t cb_index = 0;
  if (args.Length() == 2) {
    cb_index = 1;
  } else if (args.Length() == 3) {
    String::Utf8Value tmppath(args[1]->ToString());
    req->tmppath = kc::strdup(*tmppath);
    cb_index = 2;
  } else if (args.Length() == 4) {
    String::Utf8Value tmppath(args[1]->ToString());
    req->tmppath = kc::strdup(*tmppath);
    req->opts = args[2]->NumberValue();
    cb_index = 3;
  }
  req->cb = Persistent<Function>::New(Handle<Function>::Cast(args[cb_index]));

  uv_work_t *uv_req = reinterpret_cast<uv_work_t*>(malloc(sizeof(uv_work_t)));
  assert(uv_req != NULL);
  uv_req->data = req;
  TRACE("uv_work_t = %p, type = %d\n", uv_req, req->type);

  int ret = uv_queue_work(uv_default_loop(), uv_req, OnWork, OnWorkDone);
  TRACE("uv_queue_work: ret = %d\n", ret);

  wrapMapReduce->Ref();
  return scope.Close(args.This());
}


void MapReduceWrap::OnWork(uv_work_t *work_req) {
  TRACE("argument: work_req = %p\n", work_req);

  kc_mapreduce_req_t *req = reinterpret_cast<kc_mapreduce_req_t*>(work_req->data);
  assert(req != NULL);

  // do operation
  switch (req->type) {
    case KC_MAPREDUCE_EXECUTE:
      {
        TRACE("mapreduce params: tmppath = %s opts = %d\n", req->tmppath, req->opts);
        const char *tmppath = (req->tmppath ? req->tmppath : "");
        req->wrapmapreduce->tune_storage(
          req->wrapmapreduce->dbnum_, req->wrapmapreduce->clim_, req->wrapmapreduce->cbnum_
        );
        //mapreduce.tune_thread(1, 1, 1);
        if (!req->wrapmapreduce->execute(req->wrapdb->db_, tmppath, req->opts)) {
          req->result = req->wrapdb->db_->error().code();
        }
        TRACE("mapreduce.execute done\n");
        break;
      }
    default:
      assert(0);
      break;
  }
}

void MapReduceWrap::OnWorkDone(uv_work_t *work_req) {
  HandleScope scope;
  TRACE("argument: work_req = %p\n", work_req);

  kc_mapreduce_req_t *req = reinterpret_cast<kc_mapreduce_req_t*>(work_req->data);
  assert(req != NULL);

  // init callback arguments.
  int32_t argc = 0;
  Local<Value> argv[3] = { 
    Local<Value>::New(Null()),
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
    default:
      break;
  }


  //
  // execute callback
  //

  if (!req->cb.IsEmpty()) {
    TryCatch try_catch;
    MakeCallback(req->wrapmapreduce->handle_, req->cb, argc, argv);
    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }
  } 


  //
  // releases
  //
  req->wrapmapreduce->Unref();
  req->cb.Dispose();

  switch (req->type) {
    case KC_MAPREDUCE_EXECUTE:
      {
        req->wrapmapreduce->emit_.Dispose();
        req->wrapmapreduce->iter_.Dispose();
        req->wrapdb->Unref();
        req->wrapdb = NULL;
        SAFE_REQ_ATTR_FREE(req, tmppath);
        free(req);
        break;
      }
    default:
      assert(0);
  }

  req->wrapmapreduce = NULL;
  work_req->data = NULL;

  free(work_req);
}

void MapReduceWrap::OnAsyncRequestNotifier(uv_async_t *notifier, int status) {
  HandleScope scope;
  TRACE("notifier = %p, status = %d\n", notifier, status);
  assert(notifier->data != NULL);

  kc_async_mapreduce_req_t *req = reinterpret_cast<kc_async_mapreduce_req_t*>(notifier->data);
  assert(req != NULL);

  // 
  // callback
  //
  Local<Function> cb = Local<Function>::New(req->cb);

  // 
  // set arguments
  //
  int32_t argc = 0;
  Local<Value> argv[3] = { 
    Local<Value>::New(Null()),
    Local<Value>::New(Null()),
    Local<Value>::New(Null()),
  };
  switch (req->type) {
    case KC_ASYNC_MAPREDUCE_MAP:
      {
        TRACE(
          "req(%p): field1 = %s, field1_size = %ld, field2 = %s, field2_size = %ld, wrapmapreduce = %p\n", 
          req, req->field1, req->field1_size, req->field2, req->field2_size, req->wrapmapreduce
        );
        argv[argc++] = String::New(req->field1, req->field1_size);
        argv[argc++] = String::New(req->field2, req->field2_size);
        argv[argc++] = Local<Function>::New(req->wrapmapreduce->emit_);
        break;
      }
    case KC_ASYNC_MAPREDUCE_REDUCE:
      {
        TRACE(
          "req(%p): field1 = %s, field1_size = %ld, wrapmapreduce = %p\n", 
          req, req->field1, req->field1_size, req->wrapmapreduce
        );
        argv[argc++] = String::New(req->field1, req->field1_size);
        argv[argc++] = Local<Function>::New(req->wrapmapreduce->iter_);
        break;
      }
    case KC_ASYNC_MAPREDUCE_LOG:
      {
        TRACE(
          "req(%p): field1 = %s, field1_size = %ld, field2 = %s, field2_size = %ld, wrapmapreduce = %p\n", 
          req, req->field1, req->field1_size, req->field2, req->field2_size, req->wrapmapreduce
        );
        argv[argc++] = String::New(req->field1, req->field1_size);
        argv[argc++] = String::New(req->field2, req->field2_size);
        break;
      }
    case KC_ASYNC_MAPREDUCE_PRE:
    case KC_ASYNC_MAPREDUCE_MID:
    case KC_ASYNC_MAPREDUCE_POST:
      {
        TRACE("req(%p): wrapmapreduce = %p\n", req, req->wrapmapreduce);
        break;
      }
    default:
      assert(0);
      break;
  }

  //
  // exiecute
  //
  TryCatch try_catch;
  Handle<Value> ret = cb->Call(req->wrapmapreduce->handle_, argc, argv);
  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }

  if (!ret.IsEmpty() && ret->IsBoolean()) {
    req->ret = ret->BooleanValue();
  }

  //
  // notify
  //
  int32_t code = pthread_cond_signal(&async_mapreduce_cond);
  TRACE("cond siglal: code = %d\n", code);
  req->done.set(1);
}

void MapReduceWrap::Init(Handle<Object> target) {
  TRACE("load mapreduce module\n");
   
  doublesec2timespec(0.001, &ts);
  uv_async_init(
    uv_default_loop(), 
    &async_mapreduce_req_notifier, 
    OnAsyncRequestNotifier
  );
  uv_unref(reinterpret_cast<uv_handle_t*>(&async_mapreduce_req_notifier));

  // prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("MapReduce"));

  Local<ObjectTemplate> insttpl = tpl->InstanceTemplate();
  insttpl->SetInternalFieldCount(1);

  // prototype(s)
  Local<ObjectTemplate> prottpl = tpl->PrototypeTemplate();
  DEFINE_JS_METHOD(prottpl, "emit", Emit);
  DEFINE_JS_METHOD(prottpl, "iter", Iterate);
  DEFINE_JS_METHOD(prottpl, "execute", Execute);

  Persistent<Function> ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("MapReduce"), ctor);
}

