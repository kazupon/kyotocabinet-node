BUILDTYPE = Release
REPORTER = spec

ifeq (${BUILDTYPE},Debug)
GYP_BUILD_TYPE = --d
else
GYP_BUILD_TYPE = --r
BUILDTYPE = Release
endif

SRCS := src/utils.cc \
	    src/error_wrap.cc \
	    src/visitor_wrap.cc \
	    src/async.cc \
	    src/mapreduce_wrap.cc \
	    src/cursor_wrap.cc \
	    src/polydb_wrap.cc \
	    src/kyotocabinet.cc

TARGET = build/$(BUILDTYPE)/kyotocabinet.node
DEPS_TARGET = deps/kyotocabinet/libkyotocabinet.a


all: $(DEPS_TARGET) $(TARGET)

$(DEPS_TARGET):
	node-gyp rebuild ${GYP_BUILD_TYPE}

$(TARGET): $(SRCS)
	node-gyp build ${GYP_BUILD_TYPE}

clean:
	node-gyp clean
	rm -rf build

distclean: clean
	rm -rf node_modules

test:
	./node_modules/.bin/mocha --reporter ${REPORTER} --timeout 10000 test/*.js

.PHONY: test node_modules distclean all
