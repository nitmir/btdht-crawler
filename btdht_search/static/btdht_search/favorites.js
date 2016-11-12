var FAVORITE_LAST_VERSION = 1;
function get_favorites(){
    var favorites = $.cookie('favorites');
    if(favorites != null){
        favorites = JSON.parse(favorites);
    } else {
        favorites = [];
    }
    return update_favorites(favorites);
}

function update_favorite(favorite){
    if(favorite[2] == null){
        return [favorite[0], favorite[1] + '/0', FAVORITE_LAST_VERSION];
    } else {
        return favorite;
    }
}

function update_favorites(favorites){
    var nb_fav = favorites.length;
    var update = false;
    for (var i = 0; i < nb_fav; i++){
        if(favorites[i][2] != FAVORITE_LAST_VERSION){
            favorites[i] = update_favorite(favorites[i]);
            update = true;
        }
    }
    if(update){
        $.cookie('favorites', JSON.stringify(favorites), { expires: 365 * 100, path: '/' });
    }
    return favorites
}
function populate_favorite_navbar(){
    var favorites = get_favorites();
    favorites = update_favorites(favorites);
    var nb_fav = favorites.length;
    if(nb_fav < 1){
        $("#favorites_navbar").hide();
    } else {
        $("#favorites_navbar").show();
    }
    $("#favorites_dropdown").empty();
    for (var i = 0; i < nb_fav; i++){
        $("#favorites_dropdown").append('<li><a href="'+favorites[i][1]+'">'+favorites[i][0]+"</a></li>");
    }
}
function init_search_fav(){
    $("#add_fav").click(add_favorite_search);
    $("#del_fav").click(del_favorite_search);
    if(is_fav()){
        $("#del_fav").show();
    } else {
        $("#add_fav").show();                                                                                                                                                                                      
    }
}
function is_fav(){
    var favorites = get_favorites();
    var nb_fav = favorites.length;
    for (var i = 0; i < nb_fav; i++){
        if(favorites[i][1] == window.location.pathname){
            return true;
        }
    }
    return false;
}
function add_favorite_search(){
    var fav_name = prompt("Name this favorite");
    if(fav_name != null){
        var favorites = get_favorites();
        favorites.push([fav_name, window.location.pathname, FAVORITE_LAST_VERSION]);
        favorites.sort(function(x,y){if(x[0] < y[0]){ return -1; } else { if(x[0] == y[0]) return 0; else return 1;}});
        $.cookie('favorites', JSON.stringify(favorites), { expires: 365 * 100, path: '/' });
        $("#add_fav").hide();
        $("#del_fav").show();
        populate_favorite_navbar();
    }
}

function del_favorite_search(){
    var favorites = get_favorites();
    var nb_fav = favorites.length;
    for (var i = 0; i < nb_fav; i++){
        if(favorites[i] != null){
            if(favorites[i][1] == window.location.pathname){
                favorites.splice(i, 1);
                i--;
            }
        }
    }
    $.cookie('favorites', JSON.stringify(favorites), { expires: 365 * 100, path: '/' });
    $("#del_fav").hide();
    $("#add_fav").show();
    populate_favorite_navbar();
}

