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


class CursorWrap;
class AsyncFileProcessor;


class PolyDBWrap : public ObjectWrap {
  friend class CursorWrap;
  public:
    static void Init(Handle<Object> target);
    PolyDB::Cursor* Cursor();

  private:
    PolyDB *db_;
    AsyncFileProcessor *fproc_;

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
    static Handle<Value> Remove(const Arguments &args);
    static Handle<Value> Replace(const Arguments &args);
    static Handle<Value> Seize(const Arguments &args);
    static Handle<Value> Increment(const Arguments &args);
    static Handle<Value> IncrementDouble(const Arguments &args);
    static Handle<Value> Cas(const Arguments &args);
    static Handle<Value> Count(const Arguments &args);
    static Handle<Value> Size(const Arguments &args);
    static Handle<Value> Path(const Arguments &args);
    static Handle<Value> Status(const Arguments &args);
    static Handle<Value> Check(const Arguments &args);
    static Handle<Value> GetBulk(const Arguments &args);
    static Handle<Value> SetBulk(const Arguments &args);
    static Handle<Value> RemoveBulk(const Arguments &args);
    static Handle<Value> MatchPrefix(const Arguments &args);
    static Handle<Value> MatchRegex(const Arguments &args);
    static Handle<Value> MatchSimilar(const Arguments &args);
    static Handle<Value> Copy(const Arguments &args);
    static Handle<Value> Merge(const Arguments &args);
    static Handle<Value> DumpSnapshot(const Arguments &args);
    static Handle<Value> LoadSnapshot(const Arguments &args);
    static Handle<Value> Accept(const Arguments &args);
    static Handle<Value> AcceptBulk(const Arguments &args);
    static Handle<Value> Iterate(const Arguments &args);
    static Handle<Value> BeginTransaction(const Arguments &args);
    static Handle<Value> EndTransaction(const Arguments &args);
    static Handle<Value> Synchronize(const Arguments &args);
    static Handle<Value> Occupy(const Arguments &args);

    static void OnWork(uv_work_t *work_req);
    static void OnWorkDone(uv_work_t *work_req);

    //Local<Value> MakeErrorObject(PolyDB::Error::Code result);
};

#endif /* POLYDB_WRAP_H */
