function names = GetNames(T, suffix)
    path = strcat('../../../', T, '/');
    info = dir(strcat(path, '*.',  suffix));
    names = cell(length(info), 1);
    for i = 1:length(info)
        names{i} = info(i).name;
    end
end
