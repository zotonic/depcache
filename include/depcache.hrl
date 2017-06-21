%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2009-2010  Marc Worrell
%% @copyright 2014 Arjan Scherpenisse
%%
%% @doc In-memory caching server with dependency checks and local in process memoization of lookups.

%% Copyright 2009-2010 Marc Worrell, 2014 Arjan Scherpenisse
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%     http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @doc Record which depcache:memo can optionally return so that the
%% memoization function can influence the dependencies and max_age.
-record(memo, {value, max_age, deps=[]}).

