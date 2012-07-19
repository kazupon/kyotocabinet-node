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


class CursorWrap : public ObjectWrap {
  public:
    static void Init(Handle<Object> target);
  private:
    PolyDB::Cursor *cursor_;

    CursorWrap(PolyDB::Cursor *cursor);
    ~CursorWrap();

    static Handle<Value> New(const Arguments &args);
    static Handle<Value> Get(const Arguments &args);
};

#endif /* CURSOR_WRAP_H */
