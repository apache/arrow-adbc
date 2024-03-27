# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Generate Mermaid diagrams statically.  Sphinx has a mermaid
# extension, but this causes issues with the page shifting during
# load.
# First: npm install -g @mermaid-js/mermaid-cli
# (if you are using Conda, this will not be "global" but rather install to
# your Conda prefix)
# Use as: make -f mermaid.makefile -j all

MERMAID := $(shell find source/ -type f -name '*.mmd')
DIAGRAMS := $(patsubst %.mmd,%.mmd.svg,$(MERMAID))

define LICENSE
endef

%.mmd.svg : %.mmd
# XXX: mermaid doesn't properly handle comments in all layouts (the parser is
# written entirely from scratch each time, it looks like), so strip them
# manually
	grep -E -v "^%" $< | mmdc --input - --output $@
# Prepend the license header
	mv $@ $@.tmp
	echo "<!--" >> $@
	echo "  Licensed to the Apache Software Foundation (ASF) under one" >> $@
	echo "  or more contributor license agreements.  See the NOTICE file" >> $@
	echo "  distributed with this work for additional information" >> $@
	echo "  regarding copyright ownership.  The ASF licenses this file" >> $@
	echo "  to you under the Apache License, Version 2.0 (the" >> $@
	echo "  \"License\"); you may not use this file except in compliance" >> $@
	echo "  with the License.  You may obtain a copy of the License at" >> $@
	echo "" >> $@
	echo "    http://www.apache.org/licenses/LICENSE-2.0" >> $@
	echo "" >> $@
	echo "  Unless required by applicable law or agreed to in writing," >> $@
	echo "  software distributed under the License is distributed on an" >> $@
	echo "  \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY" >> $@
	echo "  KIND, either express or implied.  See the License for the" >> $@
	echo "  specific language governing permissions and limitations" >> $@
	echo "  under the License." >> $@
	echo "-->" >> $@
	cat $@.tmp >> $@
	echo "" >> $@
	rm -f $@.tmp

.PHONY: all

all : $(DIAGRAMS)
