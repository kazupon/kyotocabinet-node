/*
 * error wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include "error_wrap.h"
#include <kcpolydb.h>
#include <assert.h>
#include "utils.h"

using namespace v8;
using namespace kyotocabinet;


ErrorWrap::ErrorWrap() {};
ErrorWrap::~ErrorWrap() {};

Handle<Value> ErrorWrap::New(const Arguments &args) {
  HandleScope scope;

  ErrorWrap *obj = new ErrorWrap();
  obj->Wrap(args.This());

  return args.This();
}


void ErrorWrap::Init(Handle<Object> target) {
  // prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("Error"));

  Persistent<Function> ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("Error"), ctor);

  DEFINE_JS_CONSTANT(ctor, "SUCCESS", PolyDB::Error::SUCCESS);
  DEFINE_JS_CONSTANT(ctor, "NOIMPL", PolyDB::Error::NOIMPL);
  DEFINE_JS_CONSTANT(ctor, "INVALID", PolyDB::Error::INVALID);
  DEFINE_JS_CONSTANT(ctor, "NOREPOS", PolyDB::Error::NOREPOS);
  DEFINE_JS_CONSTANT(ctor, "NOPERM", PolyDB::Error::NOPERM);
  DEFINE_JS_CONSTANT(ctor, "BROKEN", PolyDB::Error::BROKEN);
  DEFINE_JS_CONSTANT(ctor, "DUPREC", PolyDB::Error::DUPREC);
  DEFINE_JS_CONSTANT(ctor, "NOREC", PolyDB::Error::NOREC);
  DEFINE_JS_CONSTANT(ctor, "LOGIC", PolyDB::Error::LOGIC);
  DEFINE_JS_CONSTANT(ctor, "SYSTEM", PolyDB::Error::SYSTEM);
  DEFINE_JS_CONSTANT(ctor, "MISC", PolyDB::Error::MISC);
}

