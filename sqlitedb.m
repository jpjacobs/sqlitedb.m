classdef sqlitedb <matlab.mixin.Copyable
    % Handles DB stuff for sqlite results database
    % TODO's soon
    % - add meta table storing datetime (date(now,30)) the result was added, etc etc
    % - multiple tables (also for ColNames,Types, extra property Tables)
    % - multiple injection using non-scalar structs.
    % - define dependency on data, hashing the data for integrity.
    % - wrap libsqlite3 errors
    % TODO's in the future:
    % - Separate off in objects:
    %   - Database
    %   - Tables
    %   - Queries
    properties
        Tmp         % Temporary file for serialization
        DB          % File name of the database
        Types       % Types per column
        ColNames    % Names per column
        Files       % Filenames of experimental scripts
        dbHdl       % Handle to the database
        timeout     % Timeout for executing query [default 100s]
        DiffCmd     % Command for diff
        Md5sumCmd   % Command for md5sum
    end
    properties(GetAccess = protected)

    end
    
    methods
        function obj = sqlitedb(fn,meta,tmp,timeout)
            % obj = sqlitedb(database, tmp, timeout) Make a new DB object
            % Make a new SQLite3 database object.
            % database is the filename of the database. 
            % If given, tmp will be used as temporary file for
            % serialization of datatypes not supported by SQLite3. If not
            % given, a temporary filename will be picked by tempname.
            % (Notice that if you have an SSD, you'd want to have the
            % tempfile there).
            % timeout specifies the timeout in miliseconds for the sqlite3 
            % driver to wait when another process has locked the database.
            % Since we don't want to loose data, default is 100000.
            if     nargin < 2
                meta =struct();
                tmp=tempname();
                timeout=100000;
            elseif nargin < 3
                tmp=tempname();
                timeout=100000;
            elseif nargin < 4
                timeout=100000;
            end
            obj.timeout = timeout;
            obj.Tmp = tmp;
            obj.DB  = fn;
            if ~isfield(meta,'DiffCmd')
                obj.DiffCmd = 'diff -u';
            else
                obj.DiffCmd  = meta.DiffCmd;
            end
            if ~isfield(meta,'Md5sumCmd')
                obj.Md5sumCmd = 'md5sum';
            else
                obj.Md5sumCmd = meta.Md5sumCmd;
            end

            % Initialize the database. If already existing, open it for
            % writing. If the database does not exist yet, it is
            % initialized and a table records inserted according to the
            % variables in vars, and a table "info", with fields id,txt is
            % created, and desc inserted into it. This table is meant for
            % keeping experimental description.
            % vars is a Nx2 cell array of column names and their types.

            if isfield(meta,'vars')
                fields = meta.vars(:,1)';
                types  = meta.vars(:,2)';
                head ='CREATE TABLE records (';
                % Fields and types plus the obligatory ID
                obj.Types=[{'INTEGER PRIMARY KEY AUTOINCREMENT'},types];
                obj.ColNames = [{'id'},fields];
                % Glue them together
                cols = cell(length(fields));
                for k=1:length(obj.Types)
                    cols{k} = [', "',obj.ColNames{k},'" ',obj.Types{k}];
                end
                % Close the deal
                foot = ')';
                body=[cols{:}];
                % Piece everything together (nicking of the first comma)
                sql = [head,body(2:end),foot];
            else
                sql =[];
            end

            if exist(obj.DB,'file')
                obj.open(); % Open the database
                if ~isempty(sql) % Meta.vars was given, check whether corresponding to db.
                    res = obj.execute('SELECT sql FROM sqlite_master WHERE name ==''records''');
                    if ~isequal(sql,strrep(res.sql,sprintf('\n'),''))
                        error('Schema for records not corresponding to passed column types and DB scheme');
                    end
                else
                    sql = strsplit(obj.execute('SELECT sql FROM sqlite_master WHERE name ==''records''').sql,',');
                    coltype_cell = regexp(sql,'"([A-z_]*)"\s*([A-z_ ])*)?$','tokens');
                    tmp  = cellfun(@(x)x{1},coltype_cell,'uniformoutput',false);
                    vars = cat(1,tmp{:});clear tmp coltype_cell;

                    obj.ColNames = vars(:,1)';
                    obj.Types    = vars(:,2)';
                end
                files = obj.execute('SELECT DISTINCT name from files');
                obj.Files = sort({files.name});
            else 
                % Use low-level methods for initializing the database, avoiding problems
                obj.dbHdl=sqlite3.open(obj.DB);
                sqlite3.timeout(obj.dbHdl,obj.timeout);
                % Initialize a new database file
                %
                % Files are saved as entirely for the first time (ref = NULL) and as patches afterwards
                % along with this, the filename and date are saved too.
                % Reference is always taken against the first file registered (so the one with ref == NULL
                % and the same file name).
                % An entry for each file is made each time an item is added to the database:
                %   - if no entry exsists with the same filename, the entire file is added
                %   - if there is an entry with the same name, but a different content, a patch is added
                %   - if there have been no changes (ie. no change in md5sum), the previous version is maintained.
                filessql = ['CREATE TABLE files (id INTEGER PRIMARY KEY AUTOINCREMENT',...
                    ',ref INTEGER, name TEXT, md5sum TEXT, content TXT, date TEXT);'];
                sqlite3.execute(obj.dbHdl,filessql);
                obj.Files = sort(meta.files);
                for f = obj.Files
                    fn = f{:};
                    [st,md5]=system([obj.Md5sumCmd,' ',fn]);
                    if st~= 0; error('sqlitedb:md5error','Md5sum returned an error');end
                    cont = fileread(fn);
                    sqlite3.execute(obj.dbHdl,...
                    'INSERT INTO files VALUES (NULL,NULL,?,?,?, strftime(''%Y-%m-%dT%H:%M:%SZ''))',fn,md5(1:32),cont);
                end
                % Set up multi-to-multi lookup table for linking files and records.
                filesetsSql = ['CREATE TABLE filesets (id INTEGER PRIMARY KEY AUTOINCREMENT',...
                    ',fileId NUMBER,resultId NUMBER)'];
                sqlite3.execute(obj.dbHdl,filesetsSql);
                
                % Execute the statement prepared above
                sqlite3.execute(obj.dbHdl,sql);
                sqlite3.close(obj.dbHdl);
                obj.dbHdl = [];
            end
        end
        
        function [obj] = add(obj,fields)
            % Insert raw fields into the database.
            % Fields must be complete, eg a value for each column must be
            % present. For more control, use addFrom or execute.
            if isempty(obj.dbHdl); error('Open or Init DB first'); end
            head ='INSERT into records VALUES (';
            body = repmat('?,',[1,length(obj.ColNames)]);
            args = cell(1,length(obj.ColNames));
            for k=2:length(args) % Skip first fields: id
                    args{k} = obj.ser(fields{k});
            end    
            foot = ');';
            sql=[head,body(1:end-1),foot];
            obj.execute(sql,args{:});
            recordId = obj.execute('select seq from sqlite_sequence where name = ''records'';');
            % Juggle files.
            for f=obj.Files
                fn = f{:};
                [st,md5]=system([obj.Md5sumCmd,' ',fn]);
                while length(md5) < 32
                    if st~= 0;
                        error('sqlitedb:md5error',['Md5sum returned an error:',md5]);
                    end
                    [st,md5]=system([obj.Md5sumCmd,' ',fn]);
                end
                fileId = obj.execute('SELECT id FROM files WHERE name=? AND md5sum=?;', fn, md5(1:32));
                if isempty(fileId)
                    origFile = obj.execute(... REF is NULL : get original
                    'SELECT id,content FROM files WHERE name=? AND ref IS NULL;',fn);
                    % Write original as temporary file
                    tmpOrigF = tempname();
                    fh = fopen(tmpOrigF,'w');
                    fprintf(fh,'%s',origFile.content);
                    fclose(fh);
                    % Take diff with current version
                    [stat,res]=system([obj.DiffCmd,' ',tmpOrigF,' ',fn]);
                    % insert results in the DB and get Id this row in files
                    obj.execute(...
                    'INSERT INTO files VALUES(NULL,?,?,?,?,strftime(''%Y-%m-%dT%H:%M:%SZ''))',...
                    origFile.id,fn,md5(1:32),res);
                    fileId =obj.execute('select seq from sqlite_sequence where name = ''files'';');
                else %provide compatibility between id resulting from last query above if, to seq resulting
                     % from the last one in the line above.
                    fileId.seq=fileId.id(1);
                end
                % Insert link between file and record in filesets.
                obj.execute(....
                'INSERT INTO filesets VALUES(NULL,?,?)',fileId.seq,recordId.seq);
            end
        end
        
        function [obj] = addFrom(obj,str)%,tbl)
            % Add all elements in the columns present in the struct to the
            % database.
            if ~ isscalar(str); error('sqlitedb:NonScalarStruct','Only scalar structs allowed'); end
            args = cell(1,length(obj.ColNames));
            for k=2:length(args) % 2 and k-1 below because id should not be in the str.
                if isfield(str,obj.ColNames{k})
                    args{k} = str.(obj.ColNames{k});
                else % if not present in the struct, just add a NULL
                    args{k} =[];
                end
            end
            [obj] = obj.add(args);
        end

        function [res] = getFrom(obj,str,what);
            % Check whether data in struct str is already in the database obj and return result
            % This checks all columns present in both database and struct for equality. 
            % If, for instance, the struct contains only the id column, this will return the
            % entry with that id, if existing.
            % "what" indicates what columns to fetch, in a string separated by columns (exactly like in
            % SQL) and defaults to 'id' for speed. Fetching all columns is done by using '*'.
            %
            % Example usage:
            %   - skip experiments already in the database by using
            %       if ~isempty(db.getFrom(str)); continue; end
            %   - error out upon before adding duplicates:
            %       if ~isempty(db.getFrom(str)); error('Already present'); end
            %
            if nargin < 3 % Set default "what" argument
                what = 'id';
            end
            if ~isscalar(str) % Only defined for scalar structs
                error('sqlitedb:NonScalarStruct','Struct for completeFrom needs to be scalar')
            end

            fn=intersect(obj.ColNames,fieldnames(str));
            quer = ['select ',what, ' from records where '];
            vals = {};
            % convert any vals which would go to blob to blob.
            for k = fn'
                if ~isequal(str.(k{:}),obj.ser(str.(k{:})))
                    warning('sqlitedb:BlobNotSupported',...
                    ['NYI: Datatypes mapped to BLOB not yet supported, ignored column ',k{:}])
                    continue
                end
                if isequal(str.(k{:}),[])
                    quer = [quer,sprintf('"%s" IS NULL and ',k{:})]; %#ok<AGROW>
                else                   
                    quer = [quer,sprintf('"%s" = ? and ',k{:})]; %#ok<AGROW>
                    vals = [vals,{str.(k{:})}];       %#ok<AGROW>
                end
            end
            quer = [quer(1:end-5),';'];
            res  = obj.execute(quer,vals{:});
        end

        function res = getDiffs(obj,str,getfull)
            % [res] = getDiffs (obj,str) adds a field to struct str containing results in the database
            % res will be a copy of str, with a field diff added, containing the difference vs the
            % baseline file used to obtain the result with corresponding to str.id. If str does not
            % have a field id, getFrom will be used to retrieve experiments, but results might be
            % inaccurate as this does not guarantee a unique match in the DB. In this case, the found
            % experiment id is also added to res.
            % If the option getfull is true, then get also all full files. If not, results generated
            % with base versions of files will have an empty cell as result.

            if nargin < 3
                getfull = false;
            end
            res = str; % Start from the original argument
            if ~isfield(str,'id') % locate id's of results if not given
                for k=1:length(res) % loop due to getFrom only handling scalar structs
                    found_experiments = obj.getFrom(res(k));
                    res(k).id = found_experiments.id;
                end
            end
            % Get id's as strings
            ids = cellfun(@num2str,{res.id},'uniformoutput',false);
            % id's in place, do double join to get file contents from many-to-many mapping
            SQL_1 = ['SELECT r.id AS id ,f.content AS content FROM records r ',...
                'JOIN filesets L ON (r.id = L.resultID) ',...
                'JOIN files    f ON (f.id = L.fileID  ) ',...
                'WHERE r.id IN (',strjoin(ids,', '),') '];
            if getfull
                SQL_2 = '';
            else
                SQL_2 = 'AND ref IS NOT NULL '; % if only diffs wanted.
            end
            SQL_3 = 'ORDER BY r.id;';
            files_st = obj.execute([SQL_1,SQL_2,SQL_3]);
            % As order is not necessarily the same in results and inputs, piece them together here.
            if ~isempty(files_st)
                for k=[res.id]
                    res([res.id]==k).file = {files_st([files_st.id] == k).content};
                end
            end
        end
        
        function res = purgeResults(obj,ids,chickenout)
            % purgeResults(obj,str) purges rows from records from the database which have the same id's as in ids.
            ids_str = num2str(ids(:)','%g,');
            ids_str = ids_str(1:end-1);
            % Find files which can be safely thrown away
            % Files that could be purged
            filespurge = ['SELECT fileid AS id FROM filesets WHERE resultID IN (',ids_str,')'];
            % Files that should be kept (others depend on them)
            fileskeep  = ['SELECT fileid AS id FROM filesets WHERE resultID NOT IN (',ids_str,')'];
            % files to be deleted: filespurged except fileskeep.
            del_file_ids = obj.execute([filespurge,' EXCEPT ',fileskeep,';']);
            if isempty(del_file_ids)
                del_file_ids = struct();
                del_file_ids.id =[];
            end
            % filesets to be deleted: (linked only to one resultid, so no checking of references to other results).
            del_filesets_ids = obj.execute(['SELECT id FROM filesets WHERE resultid IN (',ids_str,');']);
            if isempty(del_filesets_ids)
                del_filesets_ids = struct();
                del_filesets_ids.id =[];
            end
            % records to be deleted (already computed for above,string).
            del_records  = ids_str;
            % Do really delete?
            if nargin > 2 && ~chickenout
                fid = num2str([del_file_ids.id],'%g,');
                if ~ isempty(fid)
                    obj.execute(['DELETE FROM files WHERE id IN (',fid(1:end-1),');']);
                end
                fsid = num2str([del_filesets_ids.id],'%g,');
                if ~isempty(fsid)
                    obj.execute(['DELETE FROM filesets WHERE id IN (',fsid(1:end-1),');']);
                end
                obj.execute(['DELETE FROM records WHERE id IN (',del_records,');']);
                res=[];
            else
                res = struct();
                res.del_file_ids = [del_file_ids.id];
                res.del_filesets_ids = [del_filesets_ids.id];
                res.del_records_ids = str2num(del_records);
            end
        end

        function doub = findDoubles(obj)
            % findDoubles lists double entries from the database, if associated files are the same.
            % Returns a cell array for each set of doubles (as there can be triples, ...)
            % NOTE: Does NOT take BLOB's into account, as matlab puts in a timestamp while saving...
            fields = obj.ColNames(~ismember(obj.ColNames,'id') & ~ismember(obj.Types,'BLOB'));
            sql_1 = 'SELECT group_concat(id)';  % Get all id's 
            sql_2 = ' FROM records GROUP BY ';  % Group by all non-blob fields to get doubles
            sql_3 = strjoin(fields,',');        % 
            sql_4 = ' HAVING COUNT(*) > 1';     % It's a double if count(*) > 1
            res_str = obj.execute([sql_1,sql_2,sql_3,sql_4]);
            if isempty(res_str)
                doub = [];
            else
                doub = cellfun(@str2num,{res_str.group_concat_id},'uniformoutput',false);
            end
        end
                
        
        function obj = open(obj)
            % Open the database
            if ~isempty(obj.dbHdl);
                warning('DB already open');
            else
                obj.dbHdl=sqlite3.open(obj.DB);
                sqlite3.timeout(obj.dbHdl,obj.timeout);
                % Load the description into the object
                tmp = sqlite3.execute(obj.dbHdl,'PRAGMA TABLE_INFO(records)');
                tmp2 = struct2cell(tmp);
                obj.Types = tmp2(ismember(fieldnames(tmp),'type'),:);
                obj.ColNames = tmp2(ismember(fieldnames(tmp),'name'),:);
            end
        end
        
        function obj = close(obj)
            % Close the database
            if isempty(obj.dbHdl); warning('DB already closed'); end
            sqlite3.close(obj.dbHdl);
            % Wipe the handle so we can open again.
            obj.dbHdl = [];
        end
        
        function delete(obj)
            % when deleting the object, close the database and delete the temp file.
            % don't invoke obj.close, as this might generate a confusing warning.
            if ~isempty(obj.dbHdl);
                sqlite3.close(obj.dbHdl);
                obj.dbHdl=[];
            end
            tmpfn = [obj.Tmp,'.mat'];
            if exist(tmpfn,'file');
                delete(tmpfn);
            end
        end
        
        function res = execute(obj,sql,varargin)
            % Execute a statement, and if needed, deserialize the returned
            % values. All extra arguments are forwarded to sqlite3 execute,enabling ? expansion in the sql
            res = sqlite3.execute(obj.dbHdl,sql,varargin{:}); % Do NOT use obj.execute here -> infinite recursion
            if ~isempty(res) 
                fn=fieldnames(res);
                for m =1:numel(fn)
                    % is there a matching colum?
                    colbool = strcmpi(fn{m},obj.ColNames);
                    % 1 if the string matches but the case not.
                    if any(xor(colbool,strcmp(fn{m},obj.ColNames)))
                        newname=obj.ColNames{colbool};
                        [res.(newname)]=res.(fn{m});
                        res=rmfield(res,fn{m});
                        fn{m} = newname;
                    end
                end
                for k = 1:numel(res) % over the number of results
                    for m = 1:numel(fn) % over the columns per item
                        res(k).(fn{m})=obj.deser(res(k).(fn{m}));
                    end
                end
            else
                res = [];
            end
        end
        
        function res = select(obj,myWhere)
            % Shortcut for select statement
            res = obj.execute(sprintf('SELECT * FROM records WHERE %s ;',myWhere));
        end

    end
    
    methods(Access=protected)
        function res = ser(obj,in)
            % Serialize the value "in", such that it can be saved to
            % database.
            %
            % Scalars numbers, character vectors will be untouched, all
            % others will be saved to a temporary file, and read in as a
            % uint8. Also takes advantage of compression applied ;).
            tmpfn = [obj.Tmp,'.mat'];
            % serialize/deserialize object by using save
            if isscalar(in) && (isnumeric(in)||ischar(in))
                % Numeric and character scalers are no issue
                res= in;
            elseif ischar(in) && isequal(size(in),[1 length(in)])
                res = in;
            elseif isequal(in,[])
                res=in;
            else
                % freeze dry
                save(tmpfn,'in');
                % get the extract
                tmpfh = fopen(tmpfn,'r');
                stream = fread(tmpfh);
                fclose(tmpfh);
                % Return the uint8 version.
                res = uint8(stream);
            end
        end

        function res = deser(obj,in)
            % Reinstate a previously serialized value
            tmpfn = [obj.Tmp,'.mat'];
            % serialize/deserialize object by using save
            if isscalar(in) && (isnumeric(in)||ischar(in))
                % Numeric and character scalers are no issue
                res= in;
            elseif ischar(in) && isequal(size(in),[1 length(in)])
                % Strings are no problem, but only rowvectors
                res = in;
            elseif isempty(in)
                res = in;
            elseif isa(in,'uint8'); % All else, save and read;        
                % we are loading a previously serialized value
                tmpfh = fopen(tmpfn,'w');
                % Put in the powder
                fwrite(tmpfh,in);
                fclose(tmpfh);
                % Add water, and stir
                load(tmpfn);
                % tzadzaam!
                res=in;
            end
        end
    end
end

%TODO integrate with class:
%
% - Restore files from database (refusing to overwrite things!)
% - Allow opening an existing database not specifying meta
%
% - Provide integrity between records/ files/filesets tables:
%   Delete rows not being referenced except for original files
%   (or change mechanism so they only get added at first insert in records)
% - Data versioning
%   [Same for data as for files now
%   (figure out how to handle delays due to checksum)]
