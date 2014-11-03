%% TestClient  : Return the norm of the differences
function [output] = TestClient()
    output = [];
    matrix_names = GetNames('matrices', 'mtx');
    vector_names = GetNames('vectors', 'txt');
    result_names = GetNames('output', 'txt');
    for i = 1:length(matrix_names)
        matrix_path = strcat('../../../', 'matrices/', matrix_names{i});
        A = mmread(matrix_path);
        vector_path = strcat('../../../', 'vectors/', vector_names{i});
        v = fscanf(fopen(vector_path),'%f');
        result_path = strcat('../../../', 'output/', result_names{i});
        r = fscanf(fopen(result_path), '%f');
        output = [output; norm(A*v - r, 2)];
    end
end

function names = GetNames(T, suffix)
    path = strcat('../../../', T, '/');
    info = dir(strcat(path, '*.',  suffix));
    names = cell(length(info), 1);
    for i = 1:length(info)
        names{i} = info(i).name;
    end
end