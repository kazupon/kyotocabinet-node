/*
 * polydb wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef POLYDB_WRAP_H
#define POLYDB_WRAP_H

#include <node.h>
#include <kcpolydb.h>


using namespace node;
using namespace v8;
using namespace kyotocabinet;


class PolyDBWrap : public ObjectWrap {
  public:
    static void Init(Handle<Object> target);
  private:
    PolyDBWrap();
    ~PolyDBWrap();

    static Handle<Value> New(const Arguments &args);
    static Handle<Value> Open(const Arguments &args);
    static Handle<Value> Close(const Arguments &args);
    static Handle<Value> Set(const Arguments &args);
    static Handle<Value> Get(const Arguments &args);
    static Handle<Value> Clear(const Arguments &args);
    static Handle<Value> Add(const Arguments &args);
    static Handle<Value> Append(const Arguments &args);

    static Persistent<FunctionTemplate> ctor;
    static Persistent<String> code_symbol;

    static void OnWork(uv_work_t *work_req);
    static void OnWorkDone(uv_work_t *work_req);

    Local<Value> MakeErrorObject(PolyDB::Error::Code result);

    PolyDB *db_;
};

#endif /* POLYDB_WRAP_H */
