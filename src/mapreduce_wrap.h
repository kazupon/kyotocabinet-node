/*
 * mapreduce wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef MAPREDUCE_WRAP_H
#define MAPREDUCE_WRAP_H

#include <node.h>
#include <kcpolydb.h>
#include <kcdbext.h>

using namespace node;
using namespace v8;
using namespace kyotocabinet;


class MapReduceWrap : public ObjectWrap, kyotocabinet::MapReduce {
  public:
    static void Init(Handle<Object> target);

  private:
    Persistent<Function> map_;
    Persistent<Function> reduce_;
    Persistent<Function> log_;
    Persistent<Function> pre_;
    Persistent<Function> mid_;
    Persistent<Function> post_;
    Persistent<Function> emit_;
    Persistent<Function> iter_;
    int32_t dbnum_;
    int64_t clim_;
    int64_t cbnum_;
    ValueIterator *val_iter_;

    MapReduceWrap(
      Persistent<Function> &map,
      Persistent<Function> &reduce,
      Persistent<Function> &log,
      Persistent<Function> &pre,
      Persistent<Function> &mid,
      Persistent<Function> &post,
      int32_t dbnum,
      int64_t clim,
      int64_t cbnum
    );
    ~MapReduceWrap();

    bool map(const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz);
    bool reduce(const char *kbuf, size_t ksiz, ValueIterator *iter);
    bool log(const char *name, const char *message);
    bool preprocess();
    bool midprocess();
    bool postprocess();
    bool emit_public(const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz);

    static Handle<Value> New(const Arguments &args);
    static Handle<Value> Emit(const Arguments &args);
    static Handle<Value> Iterate(const Arguments &args);
    static Handle<Value> Execute(const Arguments &args);

    static void OnWork(uv_work_t *work_req);
    static void OnWorkDone(uv_work_t *work_req,int);
    static void OnAsyncRequestNotifier(uv_async_t *notifier, int status);
};

#endif /* MAPREDUCE_WRAP_H */

