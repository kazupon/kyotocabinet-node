/*
 * error wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include <kcpolydb.h>
#include "error_wrap.h"
#include <assert.h>

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

  Local<ObjectTemplate> insttpl = tpl->InstanceTemplate();
  insttpl->SetInternalFieldCount(1);

  // prototype
  Local<ObjectTemplate> prottpl = tpl->PrototypeTemplate();
  //tpl->PrototypeTemplate()->Set(String::NewSymbol("plusOne"), FunctionTemplate::New(PlusOne)->GetFunction());

  Persistent<Function> ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("Error"), ctor);
  ctor->Set(String::NewSymbol("SUCCESS"), Integer::New(PolyDB::Error::SUCCESS), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("NOIMPL"), Integer::New(PolyDB::Error::NOIMPL), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("INVALID"), Integer::New(PolyDB::Error::INVALID), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("NOREPOS"), Integer::New(PolyDB::Error::NOREPOS), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("NOPERM"), Integer::New(PolyDB::Error::NOPERM), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("BROKEN"), Integer::New(PolyDB::Error::BROKEN), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("DUPREC"), Integer::New(PolyDB::Error::DUPREC), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("NOREC"), Integer::New(PolyDB::Error::NOREC), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("LOGIC"), Integer::New(PolyDB::Error::LOGIC), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("SYSTEM"), Integer::New(PolyDB::Error::SYSTEM), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("MISC"), Integer::New(PolyDB::Error::MISC), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
}

