REBAR := $(shell which rebar3 2>/dev/null || echo ./rebar3)
REBAR_URL := https://s3.amazonaws.com/rebar3/rebar3

.PHONY: compile test

all: compile

compile: $(REBAR)
	$(REBAR) compile

test: $(REBAR)
	$(REBAR) eunit

xref: $(REBAR)
	$(REBAR) xref

dialyzer: $(REBAR)
	$(REBAR) dialyzer

edoc:
	$(REBAR) edoc
	
edoc_private:
	$(REBAR) as edoc_private edoc

clean:
	$(REBAR) clean
	rm -rf doc

./rebar3:
	erl -noshell -s inets start -s ssl start \
        -eval '{ok, saved_to_file} = httpc:request(get, {"$(REBAR_URL)", []}, [], [{stream, "./rebar3"}])' \
        -s inets stop -s init stop
	chmod +x ./rebar3
