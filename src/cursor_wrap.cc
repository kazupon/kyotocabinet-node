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

Handle<Value> CursorWrap::New(const Arguments &args) {
  HandleScope scope;
  TRACE("New\n");

  if ((args.Length() == 0)) {
    ThrowException(Exception::Error(String::New("Invalid parameter")));
    return args.This();
  }

  Local<String> ctor_sym = String::NewSymbol("constructor");
  Local<String> name_sym = String::NewSymbol("name");
  String::Utf8Value ctorName(args[0]->ToObject()->Get(ctor_sym)->ToObject()->Get(name_sym)->ToString());
  if (strcmp("DB", *ctorName)) {
    ThrowException(Exception::TypeError(String::New("Invalid parameter")));
    return args.This();
  }
  
  PolyDBWrap *dbWrap = ObjectWrap::Unwrap<PolyDBWrap>(args[0]->ToObject());
  CursorWrap *cursorWrap = new CursorWrap(dbWrap->Cursor());
  cursorWrap->Wrap(args.This());

  return args.This();
}

Handle<Value> CursorWrap::Get(const Arguments &args) {
  HandleScope scope;

  return args.This();
}

void CursorWrap::Init(Handle<Object> target) {
  TRACE("load cursor module\n");
   
  // prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("Cursor"));

  Local<ObjectTemplate> insttpl = tpl->InstanceTemplate();
  insttpl->SetInternalFieldCount(1);

  // prototype
  Local<ObjectTemplate> prottpl = tpl->PrototypeTemplate();
  tpl->PrototypeTemplate()->Set(String::NewSymbol("get"), FunctionTemplate::New(Get)->GetFunction());

  Persistent<Function> ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("Cursor"), ctor);
}

