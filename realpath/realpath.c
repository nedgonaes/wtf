#include <string.h>
#include <limits.h>
#include <errno.h>
#include <stdio.h>

char cwd[PATH_MAX];
char abspath[PATH_MAX];

void getcwd(char* c, size_t len)
{
    memmove(c, cwd, len);
    return;
}

void set_cwd(char* c)
{
    memmove(cwd, c, strlen(c));
    return;
}

int canon_path(char* rel, char* abspath, size_t abspath_sz)
{
    char* start = abspath;
    char* end = abspath + abspath_sz - 2;
    int rel_len = strnlen(rel, PATH_MAX);
    if (rel[rel_len] != '\0')
    {
        errno = ENAMETOOLONG;
        return -1;
    }
        
    if (*rel != '/')
    {
        getcwd(abspath, abspath_sz);
        abspath+=strlen(abspath);
        if (abspath[-1] != '/')
        {
            *abspath++ = '/';
        }
    }
    else
    {
        *abspath++ = '/';
    }

    //Invariant: abspath contains an abspatholute path
    
    while(rel[0] != '\0')
    {
        switch(rel[0])
        {
            char* name = rel;
            size_t name_sz = 0;

            case '/':
                ++rel;
                continue;
            case '.':
                if (rel[1] == '.' && (rel[2] == '/' || rel[2] == '\0'))
                {
                    rel += 2;

                    if (start == abspath - 1)
                    { 
                        //don't go back past the root directory.
                        continue;
                    }

                    while ((--abspath)[-1] != '/')
                    {
                        //move back one in abspath.
                    }

                    continue;
                }

                if (rel[1] == '/' || rel[1] == '\0')
                {
                    ++rel;
                    continue;
                }

            default:
                while (*rel != '/' && *rel != '\0')
                {
                    *abspath++ = *rel++;
                    if (abspath == end)
                    {
                        errno = ENAMETOOLONG;
                        return -1;
                    }
                }
                *abspath++ = '/';
        }
    }

    if (abspath[-1] == '/' && abspath != start + 1)
    {
        --abspath;
    }
    *abspath = '\0';
    return 0;
}

int main(void)
{
    printf("PATH_MAX = %d\n", PATH_MAX);
    set_cwd("/foo/bar");
    canon_path("../bar/././../../foo/bar/test/.", abspath, PATH_MAX);
    puts(abspath);
    canon_path("/bar/././../../foo/bar/test/.", abspath, PATH_MAX);
    puts(abspath);
    canon_path("/foo/bar/test/.", abspath, PATH_MAX);
    puts(abspath);
    canon_path("//////foo/bar/test/.", abspath, PATH_MAX);
    puts(abspath);
    canon_path("//////foo//bar//test", abspath, PATH_MAX);
    puts(abspath);
    canon_path("/foo/bar/test/", abspath, PATH_MAX);
    puts(abspath);
    canon_path("/foo/bar/test", abspath, PATH_MAX);
    puts(abspath);
    canon_path("/foo/bar/test///////", abspath, PATH_MAX);
    puts(abspath);
    canon_path("/..foo/.bar/test///////", abspath, PATH_MAX);
    puts(abspath);
    canon_path("/..foo/.bar../test..", abspath, PATH_MAX);
    puts(abspath);
    return 0;
}
