git log --since="2025-02-28" --until="2025-03-15" --pretty=tformat: --numstat | awk '{ add += $1; subs += $2; loc += $1 - $2 } END { printf "增加的行数：%s 删除的行数：%s 总行数：%s\n", add, subs, loc }'
git log --since="2025-03-14" --until="2025-03-15" --pretty=tformat: --numstat | awk '{ add += $1; subs += $2; loc += $1 - $2 } END { printf "3A: 增加的行数：%s 删除的行数：%s 总行数：%s\n", add, subs, loc }'
