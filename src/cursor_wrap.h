/*
 * cursor wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef CURSOR_WRAP_H
#define CURSOR_WRAP_H

#include <node.h>
#include <kcpolydb.h>

using namespace node;
using namespace v8;
using namespace kyotocabinet;


class PolyDBWrap;

class CursorWrap : public ObjectWrap {
  public:
    static Persistent<Function> ctor;

    static void Init(Handle<Object> target);

  private:
    PolyDBWrap *wrapdb_;
    PolyDB::Cursor *cursor_;

    CursorWrap(PolyDB::Cursor *cursor);
    ~CursorWrap();

    static Handle<Value> New(const Arguments &args);
    static Handle<Value> Create(const Arguments &args);
    static Handle<Value> Jump(const Arguments &args);
    static Handle<Value> JumpBack(const Arguments &args);
    static Handle<Value> Step(const Arguments &args);
    static Handle<Value> StepBack(const Arguments &args);
    static Handle<Value> Get(const Arguments &args);
    static Handle<Value> GetKey(const Arguments &args);
    static Handle<Value> GetValue(const Arguments &args);
    static Handle<Value> Remove(const Arguments &args);
    static Handle<Value> Seize(const Arguments &args);
    static Handle<Value> SetValue(const Arguments &args);

    static void OnWork(uv_work_t *work_req);
    static void OnWorkDone(uv_work_t *work_req);

    void SetWrapDB(PolyDBWrap *wrapdb);
    PolyDB::Error::Code GetErrorCode();
};

#endif /* CURSOR_WRAP_H */
